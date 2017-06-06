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

pub trait KeyGen {
    fn next(&mut self) -> Option<&[u8]>;
}

pub struct RepeatKeyGen {
    key: Vec<u8>,
    cnt: usize,
}

impl RepeatKeyGen {
    pub fn new(len: usize, cnt: usize) -> RepeatKeyGen {
        let mut keys = RepeatKeyGen {
            key: vec![0; len],
            cnt: cnt,
        };
        thread_rng().fill_bytes(&mut keys.key);
        keys
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
    pub fn new(len: usize, cnt: usize) -> IncreaseKeyGen {
        IncreaseKeyGen {
            key: vec![0; len],
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

pub struct RandomKeyGen {
    key: Vec<u8>,
    cnt: usize,
}

impl RandomKeyGen {
    pub fn new(len: usize, cnt: usize) -> RandomKeyGen {
        RandomKeyGen {
            key: vec![0; len],
            cnt: cnt,
        }
    }
}

impl KeyGen for RandomKeyGen {
    fn next(&mut self) -> Option<&[u8]> {
        if self.cnt > 0 {
            self.cnt -= 1;
            thread_rng().fill_bytes(&mut self.key);
            Some(&self.key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::{KeyGen, RepeatKeyGen, IncreaseKeyGen, RandomKeyGen};

    #[test]
    fn test_repeate_keygen() {
        let mut kg = RepeatKeyGen::new(8, 8);
        while let Some(key) = kg.next() {
            println!("{:?}", key);
        }
    }

    #[test]
    fn test_increase_keygen() {
        let mut kg = IncreaseKeyGen::new(8, 8);
        while let Some(key) = kg.next() {
            println!("{:?}", key);
        }
    }

    #[test]
    fn test_random_keygen() {
        let mut kg = RandomKeyGen::new(8, 8);
        while let Some(key) = kg.next() {
            println!("{:?}", key);
        }
    }
}
