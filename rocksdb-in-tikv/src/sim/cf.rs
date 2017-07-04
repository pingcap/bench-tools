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

use rocksdb::{DB, WriteBatch, Writable, CFHandle};
use super::key::KeyGen;
use super::val::ValGen;


pub struct ColumnFamilies {
    pub default: bool,
    pub lock: bool,
    pub raft: bool,
    pub write: bool,
}

pub fn cfs_write(db: &DB,
                 cfs: &ColumnFamilies,
                 keys_d: &mut KeyGen,
                 vals_d: &mut ValGen,
                 keys_l: &mut KeyGen,
                 vals_l: &mut ValGen,
                 keys_r: &mut KeyGen,
                 vals_r: &mut ValGen,
                 keys_w: &mut KeyGen,
                 vals_w: &mut ValGen,
                 batch_size: usize,
                 cf_handle_d: &CFHandle,
                 cf_handle_l: &CFHandle,
                 cf_handle_r: &CFHandle,
                 cf_handle_w: &CFHandle)
                 -> Result<(), String> {
    let mut finish = false;
    loop {
        let operation = WriteBatch::new();
        for _ in 0..batch_size {
            if cfs.default == true {
                if let Some(key_d) = keys_d.next() {
                    if let Some(val_d) = vals_d.next() {
                        try!(operation.put_cf(cf_handle_d, key_d, val_d));
                    }
                } else {
                    finish = true;
                    break;
                }
            }
            if cfs.lock == true {
                if let Some(key_l) = keys_l.next() {
                    if let Some(val_l) = vals_l.next() {
                        try!(operation.put_cf(cf_handle_l, key_l, val_l));
                        try!(operation.delete_cf(cf_handle_l, key_l));
                    }
                } else {
                    finish = true;
                    break;
                }
            }
            if cfs.raft == true {
                if let Some(key_r) = keys_r.next() {
                    if let Some(val_r) = vals_r.next() {
                        try!(operation.put_cf(cf_handle_r, key_r, val_r));
                    }
                } else {
                    finish = true;
                    break;
                }
            }
            if cfs.write == true {
                if let Some(key_w) = keys_w.next() {
                    if let Some(val_w) = vals_w.next() {
                        try!(operation.put_cf(cf_handle_w, key_w, val_w));
                    }
                } else {
                    finish = true;
                    break;
                }
            }
        }
        try!(db.write(operation));
        if finish {
            break;
        }
    }
    Ok(())
}
