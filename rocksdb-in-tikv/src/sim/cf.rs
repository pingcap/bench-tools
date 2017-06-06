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

use rocksdb::{DB, WriteBatch, Writable};

use super::key::KeyGen;
use super::val::ValGen;

pub fn cf_default_w(db: DB,
                    keys: &mut KeyGen,
                    vals: &mut ValGen,
                    batch_size: usize)
                    -> Result<(), String> {
    let mut finish = false;
    loop {
        let wb = WriteBatch::new();
        for _ in 0..batch_size {
            let key = match keys.next() {
                Some(v) => v,
                _ => {
                    finish = true;
                    break;
                }
            };
            let val = match vals.next() {
                Some(v) => v,
                _ => {
                    finish = true;
                    break;
                }
            };
            try!(wb.put(key, val));
        }

        try!(db.write(wb));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_lock_w(db: DB,
                 keys: &mut KeyGen,
                 vals: &mut ValGen,
                 batch_size: usize)
                 -> Result<(), String> {
    let mut finish = false;
    loop {
        let wb_put = WriteBatch::new();
        let wb_del = WriteBatch::new();
        for _ in 0..batch_size {
            let key = match keys.next() {
                Some(v) => v,
                _ => {
                    finish = true;
                    break;
                }
            };
            let val = match vals.next() {
                Some(v) => v,
                _ => {
                    finish = true;
                    break;
                }
            };
            try!(wb_put.put(key, val));
            try!(wb_del.delete(key));
        }

        
        try!(db.write(wb_put));
        try!(db.write(wb_del));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_write_w(db: DB) -> Result<(), String> {
    let _ = db;
    Err("not impl: cf_raft_w".to_string())
}

pub fn cf_raft_w(db: DB) -> Result<(), String> {
    let _ = db;
    Err("not impl: cf_raft_w".to_string())
}