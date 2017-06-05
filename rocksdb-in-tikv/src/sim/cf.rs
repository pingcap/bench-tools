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

pub fn cf_data_w(db: DB) -> Result<(), String> {
    let _ = db;
    Err("not impl: cf_data_w".to_string())
}

pub fn cf_lock_w(db: DB, keys: &mut KeyGen, vals: &mut ValGen) -> Result<(), String> {
    loop {
        let key = match keys.next() {
            Some(v) => v,
            _ => break,
        };
        let val = match vals.next() {
            Some(v) => v,
            _ => break,
        };

        let batch = WriteBatch::new();
        try!(batch.put(key, val));
        try!(db.write(batch));

        let batch = WriteBatch::new();
        try!(batch.delete(key));
        try!(db.write(batch));
    }
    Ok(())
}

pub fn cf_commit_w(db: DB) -> Result<(), String> {
    let _ = db;
    Err("not impl: cf_commit_w".to_string())
}

pub fn cf_raft_w(db: DB) -> Result<(), String> {
    let _ = db;
    Err("not impl: cf_raft_w".to_string())
}
