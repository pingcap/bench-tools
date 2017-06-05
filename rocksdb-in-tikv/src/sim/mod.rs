// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod key;
pub mod val;
pub mod cf;
pub mod txn;

use rand::{Rng, thread_rng};

#[inline]
pub fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut rand_str = vec![0; len];
    thread_rng().fill_bytes(&mut rand_str);
    rand_str
}