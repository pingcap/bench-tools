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

pub trait KeyGen<'a> {
    fn next(&mut self) -> Option<&'a[u8]>;
}

pub struct RepeatKeyGen<'a> {
    key: &'a[u8],
    times: usize
}

impl<'a> KeyGen<'a> for RepeatKeyGen<'a> {
    fn next(&mut self) -> Option<&'a[u8]> {
        if self.times == 0 {
            return None;
        }
        self.times -= 1;
        Some(self.key)
    }
}

impl<'a> RepeatKeyGen<'a> {
    pub fn new(key: &'a[u8], times: usize) -> RepeatKeyGen<'a> {
        RepeatKeyGen{key: key, times: times}
    }
}

//pub struct IncreaseKeyGen<'a> {
//    curr: &'a[u8]
//}
//
//impl<'a> KeyGen<'a> for IncreaseKeyGen<'a> {
//    fn next(&mut self) -> Option<&'a[u8]> {
//        None
//    }
//}
//
//impl<'a> IncreaseKeyGen<'a> {
//    pub fn new(key_len: usize) -> IncreaseKeyGen<'a> {
//        IncreaseKeyGen{curr: b"test"}
//    }
//}
