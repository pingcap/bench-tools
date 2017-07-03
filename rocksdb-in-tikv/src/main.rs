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

extern crate libc;
extern crate clap;
extern crate rocksdb;
extern crate toml;
extern crate rand;
#[macro_use]
extern crate log;

use std::process;
use std::time::Instant;
use std::boxed::Box;
use std::sync::Arc;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use std::str;

use clap::{Arg, App};
use rocksdb::DB;

mod sim;
mod env;
use env::utils;
use sim::key::{KeyGen, RepeatKeyGen, IncreaseKeyGen, RandomKeyGen, RaftLogKeyGen};
use sim::val::{ValGen, ConstValGen, RandValGen};
use sim::cf::{cfs_write, ColumnFamilies};
use env::{CF_WRITE, CF_RAFT, CF_LOCK, CF_DEFAULT};
use env::helper::{get_toml_string, get_toml_int};

const DEFAULT_WARMUP_COUNT: i64 = 200000;
const DEFAULT_BENCH_COUNT: i64 = 200000;
const DEFAULT_BATCH_SIZE: i64 = 128;
const DEFAULT_REGION_NUM: i64 = 100;

const ROCKSDB_DB_STATS_KEY: &'static str = "rocksdb.dbstats";
const ROCKSDB_CF_STATS_KEY: &'static str = "rocksdb.cfstats";

fn run() -> Result<usize, String> {
    let app = App::new("Rocksdb in TiKV")
        .author("PingCAP")
        .about("Benchmark of rocksdb in the sim-tikv-env")
        .arg(Arg::with_name("db_path")
            .short("d")
            .long("db")
            .takes_value(true)
            .help("rocksdb path")
            .required(true))
        .arg(Arg::with_name("config")
            .short("c")
            .long("config")
            .takes_value(true)
            .help("toml config file")
            .required(true));

    let matches = app.clone().get_matches();

    let db_path = Path::new(matches.value_of("db_path").unwrap());
    let config = match matches.value_of("config") {
        Some(path) => {
            let mut config_file = File::open(&path).expect("config open failed");
            let mut s = String::new();
            config_file.read_to_string(&mut s).expect("config read failed");
            toml::Value::Table(toml::Parser::new(&s)
                .parse()
                .expect("malformed config file"))
        }
        // Default empty value, lookup() always returns `None`.
        None => toml::Value::Integer(0),
    };

    let db_opts = utils::get_rocksdb_db_option(&config);
    let cfs_opts =
        vec![utils::CFOptions::new(CF_DEFAULT, utils::get_rocksdb_default_cf_option(&config)),
             utils::CFOptions::new(CF_LOCK, utils::get_rocksdb_lock_cf_option(&config)),
             utils::CFOptions::new(CF_WRITE, utils::get_rocksdb_write_cf_option(&config)),
             utils::CFOptions::new(CF_RAFT, utils::get_rocksdb_raftlog_cf_option(&config))];
    let db_path = db_path.clone();
    let db = Arc::new(utils::new_engine_opt(db_path.to_str()
                                                .unwrap(),
                                            db_opts,
                                            cfs_opts)
        .unwrap_or_else(|err| utils::exit_with_err(format!("{:?}", err))));



    let warmup_cnt = get_toml_int(&config,
                                  ("bench.config.warmup-count").chars().as_str(),
                                  Some(DEFAULT_WARMUP_COUNT)) as usize;
    let bench_cnt = get_toml_int(&config,
                                 ("bench.config.bench-count").chars().as_str(),
                                 Some(DEFAULT_BENCH_COUNT)) as usize;
    let batch_size = get_toml_int(&config,
                                  ("bench.config.batch-size").chars().as_str(),
                                  Some(DEFAULT_BATCH_SIZE)) as usize;
    let region_num = get_toml_int(&config,
                                  ("bench.config.region-num").chars().as_str(),
                                  Some(DEFAULT_REGION_NUM)) as usize;
    let operations = get_toml_string(&config,
                                     ("bench.config.operations").chars().as_str(),
                                     Some(String::from("d")));

    let kgen_default = get_toml_string(&config,
                                       ("defaultcf.data.key-gen").chars().as_str(),
                                       Some(String::from("random")));
    let kgen_lock = get_toml_string(&config,
                                    ("lockcf.data.key-gen").chars().as_str(),
                                    Some(String::from("random")));
    let kgen_raft = get_toml_string(&config,
                                    ("raftcf.data.key-gen").chars().as_str(),
                                    Some(String::from("random")));
    let kgen_write = get_toml_string(&config,
                                     ("writecf.data.key-gen").chars().as_str(),
                                     Some(String::from("random")));
    let vgen_default = get_toml_string(&config,
                                       ("defaultcf.data.value-gen").chars().as_str(),
                                       Some(String::from("random")));
    let vgen_lock = get_toml_string(&config,
                                    ("lockcf.data.value-gen").chars().as_str(),
                                    Some(String::from("random")));
    let vgen_raft = get_toml_string(&config,
                                    ("raftcf.data.value-gen").chars().as_str(),
                                    Some(String::from("random")));
    let vgen_write = get_toml_string(&config,
                                     ("writecf.data.value-gen").chars().as_str(),
                                     Some(String::from("random")));
    let klen_default = get_toml_int(&config,
                                    ("defaultcf.data.key-len").chars().as_str(),
                                    Some(32)) as usize;
    let klen_lock =
        get_toml_int(&config, ("lockcf.data.key-len").chars().as_str(), Some(32)) as usize;
    let klen_raft =
        get_toml_int(&config, ("raftcf.data.key-len").chars().as_str(), Some(32)) as usize;
    let klen_write =
        get_toml_int(&config, ("writecf.data.key-len").chars().as_str(), Some(32)) as usize;
    let vlen_default = get_toml_int(&config,
                                    ("defaultcf.data.value-len").chars().as_str(),
                                    Some(128)) as usize;
    let vlen_lock = get_toml_int(&config,
                                 ("lockcf.data.value-len").chars().as_str(),
                                 Some(128)) as usize;
    let vlen_raft = get_toml_int(&config,
                                 ("raftcf.data.value-len").chars().as_str(),
                                 Some(128)) as usize;
    let vlen_write = get_toml_int(&config,
                                  ("writecf.data.value-len").chars().as_str(),
                                  Some(128)) as usize;

    let mut warmup_k_d: Box<KeyGen> = key_type(&kgen_default, klen_default, warmup_cnt, region_num);
    let mut warmup_k_l: Box<KeyGen> = key_type(&kgen_lock, klen_lock, warmup_cnt, region_num);
    let mut warmup_k_r: Box<KeyGen> = key_type(&kgen_raft, klen_raft, warmup_cnt, region_num);
    let mut warmup_k_w: Box<KeyGen> = key_type(&kgen_write, klen_write, warmup_cnt, region_num);

    let mut bench_k_d: Box<KeyGen> = key_type(&kgen_default, klen_default, bench_cnt, region_num);
    let mut bench_k_l: Box<KeyGen> = key_type(&kgen_lock, klen_lock, bench_cnt, region_num);
    let mut bench_k_r: Box<KeyGen> = key_type(&kgen_raft, klen_raft, bench_cnt, region_num);
    let mut bench_k_w: Box<KeyGen> = key_type(&kgen_write, klen_write, bench_cnt, region_num);

    let mut v_d: Box<ValGen> = value_type(&vgen_default, vlen_default);
    let mut v_l: Box<ValGen> = value_type(&vgen_lock, vlen_lock);
    let mut v_r: Box<ValGen> = value_type(&vgen_raft, vlen_raft);
    let mut v_w: Box<ValGen> = value_type(&vgen_write, vlen_write);

    let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
    let lock_cf = db.cf_handle(CF_LOCK).unwrap();
    let write_cf = db.cf_handle(CF_WRITE).unwrap();
    let raft_cf = db.cf_handle(CF_RAFT).unwrap();

    let mut column_families = ColumnFamilies {
        default: false,
        lock: false,
        raft: false,
        write: false,
    };
    let command = operations.as_str();
    if command.contains("d") {
        column_families.default = true
    }
    if command.contains("l") {
        column_families.lock = true
    }
    if command.contains("r") {
        column_families.raft = true
    }
    if command.contains("w") {
        column_families.write = true
    }
    let _ = cfs_write(&db,
                      &column_families,
                      &mut *warmup_k_d,
                      &mut *v_d,
                      &mut *warmup_k_l,
                      &mut *v_l,
                      &mut *warmup_k_r,
                      &mut *v_r,
                      &mut *warmup_k_w,
                      &mut *v_w,
                      batch_size,
                      default_cf,
                      lock_cf,
                      raft_cf,
                      write_cf);

    let bench_res = cfs_write(&db,
                              &column_families,
                              &mut *bench_k_d,
                              &mut *v_d,
                              &mut *bench_k_l,
                              &mut *v_l,
                              &mut *bench_k_r,
                              &mut *v_r,
                              &mut *bench_k_w,
                              &mut *v_w,
                              batch_size,
                              default_cf,
                              lock_cf,
                              raft_cf,
                              write_cf);

    output_stats(&db);
    match bench_res {
        Ok(_) => Ok(bench_cnt),
        Err(e) => Err(e),
    }
}

fn output_stats(db: &DB) {
    if let Some(db_stats) = db.get_property_value(ROCKSDB_DB_STATS_KEY) {
        print!("{}", db_stats);
    }
    for name in db.cf_names() {
        let handler = db.cf_handle(name).expect("");
        if let Some(cf_stats) = db.get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY) {
            print!("{}", cf_stats);
        }
    }
}

fn key_type(key_gen: &str, key_len: usize, count: usize, region_num: usize) -> Box<KeyGen> {
    match key_gen {
        "repeat" => Box::new(RepeatKeyGen::new(key_len, count)),
        "increase" => Box::new(IncreaseKeyGen::new(key_len, count)),
        "random" => Box::new(RandomKeyGen::new(key_len, count)),
        "raft" => Box::new(RaftLogKeyGen::new(key_len, count, region_num)),
        _ => unreachable!("key-gen cannot be {:?}", key_gen),
    }
}

fn value_type(val_gen: &str, val_len: usize) -> Box<ValGen> {
    match val_gen {
        "const" => Box::new(ConstValGen::new(val_len)),
        "random" => Box::new(RandValGen::new(val_len)),
        _ => unreachable!("value-gen cannot be {:?}", val_gen),
    }
}

fn main() {
    let timer = Instant::now();
    match run() {
        Err(e) => {
            println!("{}", e);
            process::exit(1)
        }
        Ok(count) => {
            let elapsed = timer.elapsed();
            let tps = count as f64 /
                      (elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9);
            println!("invoke {} times in {} ms, tps: {}",
                     count,
                     elapsed.as_secs() * 1000 + (elapsed.subsec_nanos() as f64 / 1e6) as u64,
                     tps as u64);
        }
    };
}
