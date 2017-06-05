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

pub trait KeyGen {
    fn next(&mut self) -> Option<&[u8]>;
}

pub struct RepeatKeyGen {
    key: Vec<u8>,
    cnt: usize,
}

impl RepeatKeyGen {
    pub fn new(key: &[u8], cnt: usize) -> RepeatKeyGen {
        RepeatKeyGen {
            key: key.to_vec(),
            cnt: cnt,
        }
    }
}

impl KeyGen for RepeatKeyGen {
    fn next(&mut self) -> Option<&[u8]> {
        if self.cnt > 0 {
            self.cnt -= 1;
            Some(&self.key)
        } else if self.cnt == 0 {
            None
        } else {
            Some(&self.key)
        }
    }
}

// pub struct IncreaseKeyGen {
//    curr: &[u8]
// }

// impl Iterator for IncreaseKeyGen {
//    fn next(&mut self) -> Option<&[u8]> {
//        None
//    }
// }

// impl IncreaseKeyGen {
//    pub fn new(key_len: usize) -> IncreaseKeyGen {
//        IncreaseKeyGen{curr: b"test"}
//    }
// }
