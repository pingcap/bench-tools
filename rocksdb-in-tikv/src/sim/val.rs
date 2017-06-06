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

use rand::{Rng, thread_rng};

pub trait ValGen {
    fn next(&mut self) -> Option<&[u8]>;
}

pub struct ConstValGen {
    val: Vec<u8>,
}

impl ConstValGen {
    pub fn new(len: usize) -> ConstValGen {
        let mut vals = ConstValGen { val: Vec::with_capacity(len) };
        thread_rng().fill_bytes(&mut vals.val);
        vals
    }
}

impl ValGen for ConstValGen {
    fn next(&mut self) -> Option<&[u8]> {
        Some(&self.val)
    }
}
