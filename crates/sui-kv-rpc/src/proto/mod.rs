// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod sui {
    pub mod rpc {
        pub mod kv {
            pub mod v2alpha {
                include!("generated/sui.rpc.kv.v2alpha.rs");
            }
        }
    }
}
