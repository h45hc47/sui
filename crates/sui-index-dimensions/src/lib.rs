// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_types::effects::TransactionEffectsAPI;
use sui_types::full_checkpoint_content::ExecutedTransaction;
use sui_types::object::Owner;
use sui_types::transaction::TransactionDataAPI;

/// A queryable dimension for the checkpoint inverted index.
///
/// Each variant has a unique single-byte tag used as a prefix in row keys,
/// ensuring no two dimensions can produce the same encoded bytes.
///
/// Compound dimensions (MoveCall, EmitModule, EventType) use hierarchical
/// keys: each prefix level is a valid, independently queryable key. For
/// example, MoveCall encodes `[pkg_32]`, `[pkg_32][module]`, or
/// `[pkg_32][module\x00function]` depending on the query specificity.
/// The 32-byte address/package prefix is fixed-width (no separator needed),
/// and `\x00` separates variable-length components (safe because Move
/// identifiers cannot contain null bytes).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexDimension {
    Sender = 0x01,
    Recipient = 0x02,
    AffectedObject = 0x03,
    /// Compound: `[package_32]` | `[package_32][module]` | `[package_32][module\x00function]`
    MoveCall = 0x04,
    /// Compound: `[package_id_32]` | `[package_id_32][module]`
    EmitModule = 0x05,
    /// Compound: `[type_address_32]` | `[..][module]` | `[..\x00name]` | `[..\x00name\x00instantiation_bcs]`
    EventType = 0x06,
}

impl IndexDimension {
    pub fn tag_byte(self) -> u8 {
        self as u8
    }
}

/// Encode a dimension value into a row key component: `[tag_byte][value_bytes]`.
pub fn encode_dimension_key(dim: IndexDimension, value: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + value.len());
    key.push(dim.tag_byte());
    key.extend_from_slice(value);
    key
}

// --- Compound key construction helpers ---
// Used by both the write side (extract_dimensions) and the read side (filter parsing).

/// Build a MoveCall compound value at the desired specificity.
pub fn move_call_value(package: &[u8], module: Option<&str>, function: Option<&str>) -> Vec<u8> {
    let mut v = Vec::with_capacity(32 + 32);
    v.extend_from_slice(package);
    if let Some(m) = module {
        v.extend_from_slice(m.as_bytes());
        if let Some(f) = function {
            v.push(0x00);
            v.extend_from_slice(f.as_bytes());
        }
    }
    v
}

/// Build an EmitModule compound value at the desired specificity.
pub fn emit_module_value(package_id: &[u8], module: Option<&str>) -> Vec<u8> {
    let mut v = Vec::with_capacity(32 + 16);
    v.extend_from_slice(package_id);
    if let Some(m) = module {
        v.extend_from_slice(m.as_bytes());
    }
    v
}

/// Build an EventType compound value at the desired specificity.
/// `instantiation_bcs` is the BCS encoding of `Vec<TypeTag>`, used only
/// when matching a fully instantiated generic type.
pub fn event_type_value(
    type_address: &[u8],
    module: Option<&str>,
    name: Option<&str>,
    instantiation_bcs: Option<&[u8]>,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(32 + 32);
    v.extend_from_slice(type_address);
    if let Some(m) = module {
        v.extend_from_slice(m.as_bytes());
        if let Some(n) = name {
            v.push(0x00);
            v.extend_from_slice(n.as_bytes());
            if let Some(bcs) = instantiation_bcs {
                v.push(0x00);
                v.extend_from_slice(bcs);
            }
        }
    }
    v
}

/// Extract all (dimension, value) pairs from a transaction.
///
/// Returns raw dimension values suitable for encoding into row keys.
/// Compound dimensions emit entries at every prefix level so that
/// queries at any specificity are a single key lookup (no intersection).
pub fn extract_transaction_dimensions(tx: &ExecutedTransaction) -> Vec<(IndexDimension, Vec<u8>)> {
    let mut dims = Vec::new();

    // Sender
    dims.push((IndexDimension::Sender, tx.transaction.sender().to_vec()));

    // Recipient addresses from changed objects
    for (_, owner, _) in tx.effects.all_changed_objects() {
        match owner {
            Owner::AddressOwner(addr) => {
                dims.push((IndexDimension::Recipient, addr.to_vec()));
            }
            Owner::ConsensusAddressOwner { owner, .. } => {
                dims.push((IndexDimension::Recipient, owner.to_vec()));
            }
            _ => {}
        }
    }

    // Affected object IDs
    for change in tx.effects.object_changes() {
        dims.push((IndexDimension::AffectedObject, change.id.to_vec()));
    }

    // Move call — compound keys at package, module, and function levels
    for (_, package_id, module, function) in tx.transaction.move_calls() {
        let pkg = package_id.as_ref();
        dims.push((IndexDimension::MoveCall, move_call_value(pkg, None, None)));
        dims.push((
            IndexDimension::MoveCall,
            move_call_value(pkg, Some(module), None),
        ));
        dims.push((
            IndexDimension::MoveCall,
            move_call_value(pkg, Some(module), Some(function)),
        ));
    }

    // Event dimensions — compound keys at each prefix level
    for ev in tx.events.iter().flat_map(|evs| evs.data.iter()) {
        let pkg = ev.package_id.as_ref();
        let type_addr = ev.type_.address.as_ref();
        let emit_mod: &str = ev.transaction_module.as_str();
        let type_mod: &str = ev.type_.module.as_str();
        let type_name: &str = ev.type_.name.as_str();

        // EmitModule: package_id level, then package_id+module level
        dims.push((IndexDimension::EmitModule, emit_module_value(pkg, None)));
        dims.push((
            IndexDimension::EmitModule,
            emit_module_value(pkg, Some(emit_mod)),
        ));

        // EventType: address → +module → +name → +instantiation
        dims.push((
            IndexDimension::EventType,
            event_type_value(type_addr, None, None, None),
        ));
        dims.push((
            IndexDimension::EventType,
            event_type_value(type_addr, Some(type_mod), None, None),
        ));
        dims.push((
            IndexDimension::EventType,
            event_type_value(type_addr, Some(type_mod), Some(type_name), None),
        ));

        if !ev.type_.type_params.is_empty() {
            let params_bcs =
                bcs::to_bytes(&ev.type_.type_params).expect("BCS encoding of type params");
            dims.push((
                IndexDimension::EventType,
                event_type_value(
                    type_addr,
                    Some(type_mod),
                    Some(type_name),
                    Some(&params_bcs),
                ),
            ));
        }
    }

    dims
}

/// Extract per-event dimensions from a transaction.
///
/// Returns `(dimension, value, event_idx)` tuples — one entry per
/// `(event, dimension_prefix_level)`. Each event inherits the containing
/// transaction's `Sender` bits so mixed tx-level + event-level filters can
/// be evaluated entirely against the event-keyed bitmap index.
///
/// Compound dimensions (EmitModule, EventType) emit an entry at every prefix
/// level, matching the read-side filter parsing.
///
/// Only dimensions that are filter-addressable on events today are emitted:
/// `Sender` (tx sender), `EmitModule` (where emitted), `EventType` (what was
/// emitted, at every prefix level).
pub fn extract_event_dimensions(tx: &ExecutedTransaction) -> Vec<(IndexDimension, Vec<u8>, u32)> {
    let mut dims = Vec::new();

    let sender_bytes = tx.transaction.sender().to_vec();

    for (idx, ev) in tx.events.iter().flat_map(|evs| evs.data.iter()).enumerate() {
        let event_idx = idx as u32;

        // Sender inherited from the tx so sender-only filters resolve in event-space.
        dims.push((IndexDimension::Sender, sender_bytes.clone(), event_idx));

        let pkg = ev.package_id.as_ref();
        let type_addr = ev.type_.address.as_ref();
        let emit_mod: &str = ev.transaction_module.as_str();
        let type_mod: &str = ev.type_.module.as_str();
        let type_name: &str = ev.type_.name.as_str();

        // EmitModule: package_id level, then package_id+module level
        dims.push((
            IndexDimension::EmitModule,
            emit_module_value(pkg, None),
            event_idx,
        ));
        dims.push((
            IndexDimension::EmitModule,
            emit_module_value(pkg, Some(emit_mod)),
            event_idx,
        ));

        // EventType: address → +module → +name → +instantiation
        dims.push((
            IndexDimension::EventType,
            event_type_value(type_addr, None, None, None),
            event_idx,
        ));
        dims.push((
            IndexDimension::EventType,
            event_type_value(type_addr, Some(type_mod), None, None),
            event_idx,
        ));
        dims.push((
            IndexDimension::EventType,
            event_type_value(type_addr, Some(type_mod), Some(type_name), None),
            event_idx,
        ));

        if !ev.type_.type_params.is_empty() {
            let params_bcs =
                bcs::to_bytes(&ev.type_.type_params).expect("BCS encoding of type params");
            dims.push((
                IndexDimension::EventType,
                event_type_value(
                    type_addr,
                    Some(type_mod),
                    Some(type_name),
                    Some(&params_bcs),
                ),
                event_idx,
            ));
        }
    }

    dims
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dimension_tags_are_unique() {
        use std::collections::HashSet;
        let tags = [
            IndexDimension::Sender,
            IndexDimension::Recipient,
            IndexDimension::AffectedObject,
            IndexDimension::MoveCall,
            IndexDimension::EmitModule,
            IndexDimension::EventType,
        ];
        let tag_bytes: HashSet<u8> = tags.iter().map(|t| t.tag_byte()).collect();
        assert_eq!(
            tag_bytes.len(),
            tags.len(),
            "all dimension tags must be unique"
        );
    }

    #[test]
    fn test_encode_dimension_key_format() {
        let value = b"hello";
        let key = encode_dimension_key(IndexDimension::EmitModule, value);
        assert_eq!(key[0], 0x05);
        assert_eq!(&key[1..], b"hello");
    }

    #[test]
    fn test_encode_dimension_key_no_collision() {
        let value = vec![0x42; 32];
        let key1 = encode_dimension_key(IndexDimension::Sender, &value);
        let key2 = encode_dimension_key(IndexDimension::Recipient, &value);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_move_call_compound_key_hierarchy() {
        let pkg = [0xAA; 32];

        let pkg_only = move_call_value(&pkg, None, None);
        let pkg_mod = move_call_value(&pkg, Some("coin"), None);
        let pkg_mod_func = move_call_value(&pkg, Some("coin"), Some("transfer"));

        // Package-level is just the 32-byte address
        assert_eq!(pkg_only.len(), 32);
        // Module-level extends with module bytes
        assert_eq!(pkg_mod.len(), 32 + 4);
        // Function-level adds \x00 separator + function bytes
        assert_eq!(pkg_mod_func.len(), 32 + 4 + 1 + 8);
        assert_eq!(pkg_mod_func[36], 0x00);

        // Each level is a strict prefix of the next
        assert!(pkg_mod.starts_with(&pkg_only));
        assert!(pkg_mod_func.starts_with(&pkg_mod[..36]));
    }

    #[test]
    fn test_move_call_no_collision_across_modules() {
        let pkg = [0xBB; 32];
        // "ab" + function "cd" vs "a" + function "bcd"
        let key1 = move_call_value(&pkg, Some("ab"), Some("cd"));
        let key2 = move_call_value(&pkg, Some("a"), Some("bcd"));
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_event_type_compound_key_hierarchy() {
        let addr = [0xCC; 32];

        let addr_only = event_type_value(&addr, None, None, None);
        let addr_mod = event_type_value(&addr, Some("coin"), None, None);
        let addr_mod_name = event_type_value(&addr, Some("coin"), Some("CoinEvent"), None);
        let bcs_params = vec![0x01, 0x02, 0x03];
        let addr_full = event_type_value(&addr, Some("coin"), Some("CoinEvent"), Some(&bcs_params));

        assert_eq!(addr_only.len(), 32);
        assert_eq!(addr_mod.len(), 32 + 4);
        assert_eq!(addr_mod_name.len(), 32 + 4 + 1 + 9);
        assert_eq!(addr_full.len(), 32 + 4 + 1 + 9 + 1 + 3);

        // Separators at the right positions
        assert_eq!(addr_mod_name[36], 0x00); // between module and name
        assert_eq!(addr_full[46], 0x00); // between name and instantiation
    }

    #[test]
    fn test_emit_module_compound_key() {
        let pkg = [0xDD; 32];

        let pkg_only = emit_module_value(&pkg, None);
        let pkg_mod = emit_module_value(&pkg, Some("transfer"));

        assert_eq!(pkg_only.len(), 32);
        assert_eq!(pkg_mod.len(), 32 + 8);
        assert!(pkg_mod.starts_with(&pkg_only));
    }
}
