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

pub trait ValGen<'a> {
    fn next(&mut self) -> Option<&'a[u8]>;
}

pub struct ConstValGen<'a> {
    val: &'a[u8],
}

impl<'a> ValGen<'a> for ConstValGen<'a> {
    fn next(&mut self) -> Option<&'a[u8]> {
        Some(self.val)
    }
}

impl<'a> ConstValGen<'a> {
    pub fn new(val: &'a[u8]) -> ConstValGen<'a> {
        ConstValGen{val: val}
    }
}
