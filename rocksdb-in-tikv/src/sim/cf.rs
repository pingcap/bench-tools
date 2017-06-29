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

pub fn cf_d_write(db: &DB,
                  keys_d: &mut KeyGen,
                  vals_d: &mut ValGen,
                  batch_size: usize,
                  cf_handle: &CFHandle)
                  -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_d = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_d) = keys_d.next() {
                if let Some(val_d) = vals_d.next() {
                    try!(put_d.put_cf(cf_handle, key_d, val_d));
                }
            } else {
                finish = true;
                break;
            }
        }

        try!(db.write(put_d));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_l_write(db: &DB,
                  keys_l: &mut KeyGen,
                  vals_l: &mut ValGen,
                  batch_size: usize,
                  cf_handle: &CFHandle)
                  -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_l = WriteBatch::new();
        let del_l = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_l) = keys_l.next() {
                if let Some(val_l) = vals_l.next() {
                    try!(put_l.put_cf(cf_handle, key_l, val_l));
                    try!(del_l.delete_cf(cf_handle, key_l));
                }
            } else {
                finish = true;
                break;
            }
        }

        try!(db.write(put_l));
        try!(db.write(del_l));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_w_write(db: &DB,
                  keys_w: &mut KeyGen,
                  vals_w: &mut ValGen,
                  batch_size: usize,
                  cf_handle: &CFHandle)
                  -> Result<(), String> {
    cf_d_write(db, keys_w, vals_w, batch_size, cf_handle)
}

pub fn cf_r_write(db: &DB,
                  keys_r: &mut KeyGen,
                  vals_r: &mut ValGen,
                  batch_size: usize,
                  cf_handle: &CFHandle)
                  -> Result<(), String> {
    cf_d_write(db, keys_r, vals_r, batch_size, cf_handle)
}

pub fn cf_dl_write(db: &DB,
                   keys_d: &mut KeyGen,
                   vals_d: &mut ValGen,
                   keys_l: &mut KeyGen,
                   vals_l: &mut ValGen,
                   batch_size: usize,
                   cf_handle_1: &CFHandle,
                   cf_handle_2: &CFHandle)
                   -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_d = WriteBatch::new();
        let put_l = WriteBatch::new();
        let del_l = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_d) = keys_d.next() {
                if let Some(val_d) = vals_d.next() {
                    if let Some(key_l) = keys_l.next() {
                        if let Some(val_l) = vals_l.next() {
                            try!(put_d.put_cf(cf_handle_1, key_d, val_d));
                            try!(put_l.put_cf(cf_handle_2, key_l, val_l));
                            try!(del_l.delete_cf(cf_handle_2, key_l));
                        }
                    }
                }
            } else {
                finish = true;
                break;
            }
        }
        try!(db.write(put_d));
        try!(db.write(put_l));
        try!(db.write(del_l));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_dr_write(db: &DB,
                   keys_d: &mut KeyGen,
                   vals_d: &mut ValGen,
                   keys_r: &mut KeyGen,
                   vals_r: &mut ValGen,
                   batch_size: usize,
                   cf_handle_1: &CFHandle,
                   cf_handle_2: &CFHandle)
                   -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_d = WriteBatch::new();
        let put_r = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_d) = keys_d.next() {
                if let Some(val_d) = vals_d.next() {
                    if let Some(key_r) = keys_r.next() {
                        if let Some(val_r) = vals_r.next() {
                            try!(put_d.put_cf(cf_handle_1, key_d, val_d));
                            try!(put_r.put_cf(cf_handle_2, key_r, val_r));
                        }
                    }
                }
            } else {
                finish = true;
                break;
            }
        }
        try!(db.write(put_d));
        try!(db.write(put_r));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_dw_write(db: &DB,
                   keys_d: &mut KeyGen,
                   vals_d: &mut ValGen,
                   keys_w: &mut KeyGen,
                   vals_w: &mut ValGen,
                   batch_size: usize,
                   cf_handle_1: &CFHandle,
                   cf_handle_2: &CFHandle)
                   -> Result<(), String> {
    cf_dr_write(db,
                keys_d,
                vals_d,
                keys_w,
                vals_w,
                batch_size,
                cf_handle_1,
                cf_handle_2)
}

pub fn cf_lr_write(db: &DB,
                   keys_l: &mut KeyGen,
                   vals_l: &mut ValGen,
                   keys_r: &mut KeyGen,
                   vals_r: &mut ValGen,
                   batch_size: usize,
                   cf_handle_1: &CFHandle,
                   cf_handle_2: &CFHandle)
                   -> Result<(), String> {
    println!("test cf_lr_write");
    cf_dl_write(db,
                keys_r,
                vals_r,
                keys_l,
                vals_l,
                batch_size,
                cf_handle_1,
                cf_handle_2)
}

pub fn cf_lw_write(db: &DB,
                   keys_l: &mut KeyGen,
                   vals_l: &mut ValGen,
                   keys_w: &mut KeyGen,
                   vals_w: &mut ValGen,
                   batch_size: usize,
                   cf_handle_1: &CFHandle,
                   cf_handle_2: &CFHandle)
                   -> Result<(), String> {
    cf_dl_write(db,
                keys_w,
                vals_w,
                keys_l,
                vals_l,
                batch_size,
                cf_handle_1,
                cf_handle_2)
}

pub fn cf_rw_write(db: &DB,
                   keys_r: &mut KeyGen,
                   vals_r: &mut ValGen,
                   keys_w: &mut KeyGen,
                   vals_w: &mut ValGen,
                   batch_size: usize,
                   cf_handle_1: &CFHandle,
                   cf_handle_2: &CFHandle)
                   -> Result<(), String> {
    cf_dr_write(db,
                keys_r,
                vals_r,
                keys_w,
                vals_w,
                batch_size,
                cf_handle_1,
                cf_handle_2)
}

pub fn cf_dlr_write(db: &DB,
                    keys_d: &mut KeyGen,
                    vals_d: &mut ValGen,
                    keys_l: &mut KeyGen,
                    vals_l: &mut ValGen,
                    keys_r: &mut KeyGen,
                    vals_r: &mut ValGen,
                    batch_size: usize,
                    cf_handle_1: &CFHandle,
                    cf_handle_2: &CFHandle,
                    cf_handle_3: &CFHandle)
                    -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_d = WriteBatch::new();
        let put_l = WriteBatch::new();
        let del_l = WriteBatch::new();
        let put_r = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_d) = keys_d.next() {
                if let Some(val_d) = vals_d.next() {
                    if let Some(key_l) = keys_l.next() {
                        if let Some(val_l) = vals_l.next() {
                            if let Some(key_r) = keys_r.next() {
                                if let Some(val_r) = vals_r.next() {
                                    try!(put_d.put_cf(cf_handle_1, key_d, val_d));
                                    try!(put_l.put_cf(cf_handle_2, key_l, val_l));
                                    try!(del_l.delete_cf(cf_handle_2, key_l));
                                    try!(put_r.put_cf(cf_handle_3, key_r, val_r));
                                }
                            }
                        }
                    }
                }
            } else {
                finish = true;
                break;
            }
        }
        try!(db.write(put_d));
        try!(db.write(put_l));
        try!(db.write(del_l));
        try!(db.write(put_r));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_dlw_write(db: &DB,
                    keys_d: &mut KeyGen,
                    vals_d: &mut ValGen,
                    keys_l: &mut KeyGen,
                    vals_l: &mut ValGen,
                    keys_w: &mut KeyGen,
                    vals_w: &mut ValGen,
                    batch_size: usize,
                    cf_handle_1: &CFHandle,
                    cf_handle_2: &CFHandle,
                    cf_handle_3: &CFHandle)
                    -> Result<(), String> {
    cf_dlr_write(db,
                 keys_d,
                 vals_d,
                 keys_l,
                 vals_l,
                 keys_w,
                 vals_w,
                 batch_size,
                 cf_handle_1,
                 cf_handle_2,
                 cf_handle_3)
}

pub fn cf_drw_write(db: &DB,
                    keys_d: &mut KeyGen,
                    vals_d: &mut ValGen,
                    keys_r: &mut KeyGen,
                    vals_r: &mut ValGen,
                    keys_w: &mut KeyGen,
                    vals_w: &mut ValGen,
                    batch_size: usize,
                    cf_handle_1: &CFHandle,
                    cf_handle_2: &CFHandle,
                    cf_handle_3: &CFHandle)
                    -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_d = WriteBatch::new();
        let put_r = WriteBatch::new();
        let put_w = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_d) = keys_d.next() {
                if let Some(val_d) = vals_d.next() {
                    if let Some(key_r) = keys_r.next() {
                        if let Some(val_r) = vals_r.next() {
                            if let Some(key_w) = keys_w.next() {
                                if let Some(val_w) = vals_w.next() {
                                    try!(put_d.put_cf(cf_handle_1, key_d, val_d));
                                    try!(put_r.put_cf(cf_handle_2, key_r, val_r));
                                    try!(put_w.put_cf(cf_handle_3, key_w, val_w));
                                }
                            }
                        }
                    }
                }

            } else {
                finish = true;
                break;
            }
        }

        try!(db.write(put_d));
        try!(db.write(put_r));
        try!(db.write(put_w));
        if finish {
            break;
        }
    }
    Ok(())
}

pub fn cf_lrw_write(db: &DB,
                    keys_l: &mut KeyGen,
                    vals_l: &mut ValGen,
                    keys_r: &mut KeyGen,
                    vals_r: &mut ValGen,
                    keys_w: &mut KeyGen,
                    vals_w: &mut ValGen,
                    batch_size: usize,
                    cf_handle_1: &CFHandle,
                    cf_handle_2: &CFHandle,
                    cf_handle_3: &CFHandle)
                    -> Result<(), String> {
    cf_dlr_write(db,
                 keys_r,
                 vals_r,
                 keys_l,
                 vals_l,
                 keys_w,
                 vals_w,
                 batch_size,
                 cf_handle_1,
                 cf_handle_2,
                 cf_handle_3)
}

pub fn cf_dlrw_write(db: &DB,
                     keys_d: &mut KeyGen,
                     vals_d: &mut ValGen,
                     keys_l: &mut KeyGen,
                     vals_l: &mut ValGen,
                     keys_r: &mut KeyGen,
                     vals_r: &mut ValGen,
                     keys_w: &mut KeyGen,
                     vals_w: &mut ValGen,
                     batch_size: usize,
                     cf_handle_1: &CFHandle,
                     cf_handle_2: &CFHandle,
                     cf_handle_3: &CFHandle,
                     cf_handle_4: &CFHandle)
                     -> Result<(), String> {
    let mut finish = false;
    loop {
        let put_d = WriteBatch::new();
        let put_l = WriteBatch::new();
        let del_l = WriteBatch::new();
        let put_r = WriteBatch::new();
        let put_w = WriteBatch::new();
        for _ in 0..batch_size {
            if let Some(key_d) = keys_d.next() {
                if let Some(val_d) = vals_d.next() {
                    if let Some(key_l) = keys_l.next() {
                        if let Some(val_l) = vals_l.next() {
                            if let Some(key_r) = keys_r.next() {
                                if let Some(val_r) = vals_r.next() {
                                    if let Some(key_w) = keys_w.next() {
                                        if let Some(val_w) = vals_w.next() {
                                            try!(put_d.put_cf(cf_handle_1, key_d, val_d));
                                            try!(put_l.put_cf(cf_handle_2, key_l, val_l));
                                            try!(del_l.delete_cf(cf_handle_2, key_l));
                                            try!(put_r.put_cf(cf_handle_3, key_r, val_r));
                                            try!(put_w.put_cf(cf_handle_4, key_w, val_w));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            } else {
                finish = true;
                break;
            }
        }
        try!(db.write(put_d));
        try!(db.write(put_l));
        try!(db.write(del_l));
        try!(db.write(put_r));
        try!(db.write(put_w));
        if finish {
            break;
        }
    }
    Ok(())
}
