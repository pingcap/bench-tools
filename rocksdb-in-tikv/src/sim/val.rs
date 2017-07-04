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

use rand::{Rng, thread_rng, XorShiftRng, SeedableRng};

pub trait ValGen {
    fn next(&mut self) -> Option<&[u8]>;
}

pub struct RepeatValGen {
    val: Vec<u8>,
}

impl RepeatValGen {
    pub fn new(len: usize) -> RepeatValGen {
        let mut vals = RepeatValGen { val: vec![0; len] };
        thread_rng().fill_bytes(&mut vals.val);
        vals
    }
}

impl ValGen for RepeatValGen {
    fn next(&mut self) -> Option<&[u8]> {
        Some(&self.val)
    }
}

pub struct RandValGen {
    val: Vec<u8>,
    rand: XorShiftRng,
}

impl RandValGen {
    pub fn new(len: usize) -> RandValGen {
        RandValGen {
            val: vec![0; len],
            rand: XorShiftRng::from_seed([1; 4]),
        }
    }
}

impl ValGen for RandValGen {
    fn next(&mut self) -> Option<&[u8]> {
        self.rand.fill_bytes(&mut self.val);
        Some(&self.val)
    }
}

#[cfg(test)]
mod test {
    use super::{ValGen, RepeatValGen, RandValGen};

    #[test]
    fn test_const_valgen() {
        let mut vg = RepeatValGen::new(8);
        for _ in 0..8 {
            let val = vg.next().expect("");
            println!("{:?}", val);
        }
    }

    #[test]
    fn test_rand_valgen() {
        let mut vg = RandValGen::new(8);
        for _ in 0..8 {
            let val = vg.next().expect("");
            println!("{:?}", val);
        }
    }
}
