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
        } else {
            None
        }
    }
}

pub struct IncreaseKeyGen {
    key: Vec<u8>,
    cnt: usize,
}

impl IncreaseKeyGen {
    pub fn new(key: &[u8], cnt: usize) -> IncreaseKeyGen {
        IncreaseKeyGen {
            key: key.to_vec(),
            cnt: cnt,
        }
    }
    fn key_inc(&mut self) {
        let mut n = self.key.len();
        while n > 0 {
            self.key[n - 1] += 1;
            if self.key[n - 1] != 0 {
                break;
            }
            n -= 1;
        }
    }
}

impl KeyGen for IncreaseKeyGen {
    fn next(&mut self) -> Option<&[u8]> {
        if self.cnt > 0 {
            self.cnt -= 1;
            self.key_inc();
            Some(&self.key)
        } else {
            None
        }
    }
}
