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

use clap::{Arg, App, SubCommand};
use rocksdb::DB;

mod sim;
mod env;
use env::cfscfg;
use sim::key::{KeyGen, RepeatKeyGen, IncreaseKeyGen, RandomKeyGen, RaftLogKeyGen};
use sim::val::{ValGen, ConstValGen, RandValGen};
use sim::cf::{cf_d_write, cf_l_write, cf_r_write, cf_w_write, cf_dl_write, cf_dr_write,
              cf_dw_write, cf_lr_write, cf_lw_write, cf_rw_write, cf_dlr_write, cf_dlw_write,
              cf_drw_write, cf_lrw_write, cf_dlrw_write};
use env::{CF_WRITE, CF_RAFT, CF_LOCK, CF_DEFAULT};
use env::helper::{get_toml_string, get_toml_int};

const DEFAULT_COUNT: usize = 10000;
const DEFAULT_BATCH_SIZE: usize = 128;
const DEFAULT_REGION_NUM: usize = 100;

const ROCKSDB_DB_STATS_KEY: &'static str = "rocksdb.dbstats";
const ROCKSDB_CF_STATS_KEY: &'static str = "rocksdb.cfstats";

fn run() -> Result<usize, String> {
    let app = App::new("Rocksdb in TiKV")
        .author("PingCAP")
        .about("Benchmark of rocksdb in the sim-tikv-env")
        .arg(Arg::with_name("skip_sys_check")
            .short("N")
            .takes_value(false)
            .help("skip system check")
            .required(false))
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
            .required(true))
        .arg(Arg::with_name("count")
            .short("n")
            .long("count")
            .takes_value(true)
            .help("request count")
            .required(true))
        .arg(Arg::with_name("batch_size")
            .short("B")
            .long("batch_size")
            .takes_value(true)
            .help("set batch size")
            .required(false))
        .arg(Arg::with_name("region_num")
            .short("R")
            .long("region_num")
            .takes_value(true)
            .help("set region number")
            .required(false))
        .subcommand(SubCommand::with_name("cf")
            .subcommand(SubCommand::with_name("d"))
            .subcommand(SubCommand::with_name("l"))
            .subcommand(SubCommand::with_name("r"))
            .subcommand(SubCommand::with_name("w"))
            .subcommand(SubCommand::with_name("dl"))
            .subcommand(SubCommand::with_name("dr"))
            .subcommand(SubCommand::with_name("dw"))
            .subcommand(SubCommand::with_name("lr"))
            .subcommand(SubCommand::with_name("lw"))
            .subcommand(SubCommand::with_name("rw"))
            .subcommand(SubCommand::with_name("dlr"))
            .subcommand(SubCommand::with_name("dlw"))
            .subcommand(SubCommand::with_name("drw"))
            .subcommand(SubCommand::with_name("lrw"))
            .subcommand(SubCommand::with_name("dlrw")))
        .subcommand(SubCommand::with_name("txn"));

    let matches = app.clone().get_matches();

    if !matches.is_present("skip_sys_check") {
        if let Err(e) = env::check::check_system_config() {
            return Err(format!("system config not satisfied: {}\n", e));
        }
    }

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

    let db_opts = cfscfg::get_rocksdb_db_option(&config);
    let cfs_opts =
        vec![cfscfg::CFOptions::new(CF_DEFAULT, cfscfg::get_rocksdb_default_cf_option(&config)),
             cfscfg::CFOptions::new(CF_LOCK, cfscfg::get_rocksdb_lock_cf_option(&config)),
             cfscfg::CFOptions::new(CF_WRITE, cfscfg::get_rocksdb_write_cf_option(&config)),
             cfscfg::CFOptions::new(CF_RAFT, cfscfg::get_rocksdb_raftlog_cf_option(&config))];
    let db_path = db_path.clone();
    let db = Arc::new(cfscfg::new_engine_opt(db_path.to_str()
                                                 .unwrap(),
                                             db_opts,
                                             cfs_opts)
        .unwrap_or_else(|err| cfscfg::exit_with_err(format!("{:?}", err))));

    let count = match matches.value_of("count") {
        Some(v) => {
            match v.parse() {
                Ok(v) => v,
                Err(count) => return Err(format!("{} is not a number", count)),
            }
        }
        None => DEFAULT_COUNT,
    };

    let batch_size = match matches.value_of("batch_size") {
        Some(v) => {
            match v.parse() {
                Ok(v) => v,
                Err(batch_size) => return Err(format!("{} is not a number", batch_size)),
            }
        }
        None => DEFAULT_BATCH_SIZE,
    };

    let region_num = match matches.value_of("region_num") {
        Some(v) => {
            match v.parse() {
                Ok(v) => v,
                Err(region_num) => return Err(format!("{} is not a number", region_num)),
            }
        }
        None => DEFAULT_REGION_NUM,
    };

    let keygen_d = get_toml_string(&config,
                                   ("defaultcf.data.key-gen").chars().as_str(),
                                   Some(String::from("random")));
    let keygen_l = get_toml_string(&config,
                                   ("lockcf.data.key-gen").chars().as_str(),
                                   Some(String::from("random")));
    let keygen_r = get_toml_string(&config,
                                   ("raftcf.data.key-gen").chars().as_str(),
                                   Some(String::from("random")));
    let keygen_w = get_toml_string(&config,
                                   ("writecf.data.key-gen").chars().as_str(),
                                   Some(String::from("random")));
    let valgen_d = get_toml_string(&config,
                                   ("defaultcf.data.value-gen").chars().as_str(),
                                   Some(String::from("random")));
    let valgen_l = get_toml_string(&config,
                                   ("lockcf.data.value-gen").chars().as_str(),
                                   Some(String::from("random")));
    let valgen_r = get_toml_string(&config,
                                   ("raftcf.data.value-gen").chars().as_str(),
                                   Some(String::from("random")));
    let valgen_w = get_toml_string(&config,
                                   ("writecf.data.value-gen").chars().as_str(),
                                   Some(String::from("random")));
    let keylen_d = get_toml_int(&config,
                                ("defaultcf.data.key-len").chars().as_str(),
                                Some(32));
    let keylen_l = get_toml_int(&config, ("lockcf.data.key-len").chars().as_str(), Some(32));
    let keylen_r = get_toml_int(&config, ("raftcf.data.key-len").chars().as_str(), Some(32));
    let keylen_w = get_toml_int(&config, ("writecf.data.key-len").chars().as_str(), Some(32));
    let valuelen_d = get_toml_int(&config,
                                  ("defaultcf.data.value-len").chars().as_str(),
                                  Some(128));
    let valuelen_l = get_toml_int(&config,
                                  ("lockcf.data.value-len").chars().as_str(),
                                  Some(128));
    let valuelen_r = get_toml_int(&config,
                                  ("raftcf.data.value-len").chars().as_str(),
                                  Some(128));
    let valuelen_w = get_toml_int(&config,
                                  ("writecf.data.value-len").chars().as_str(),
                                  Some(128));

    let mut keys_d: Box<KeyGen> = key_type(&keygen_d, keylen_d as usize, count, region_num);
    let mut keys_l: Box<KeyGen> = key_type(&keygen_l, keylen_l as usize, count, region_num);
    let mut keys_r: Box<KeyGen> = key_type(&keygen_r, keylen_r as usize, count, region_num);
    let mut keys_w: Box<KeyGen> = key_type(&keygen_w, keylen_w as usize, count, region_num);
    let mut vals_d: Box<ValGen> = value_type(&valgen_d, valuelen_d as usize);
    let mut vals_l: Box<ValGen> = value_type(&valgen_l, valuelen_l as usize);
    let mut vals_r: Box<ValGen> = value_type(&valgen_r, valuelen_r as usize);
    let mut vals_w: Box<ValGen> = value_type(&valgen_w, valuelen_w as usize);

    let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
    let lock_cf = db.cf_handle(CF_LOCK).unwrap();
    let write_cf = db.cf_handle(CF_WRITE).unwrap();
    let raft_cf = db.cf_handle(CF_RAFT).unwrap();

    let res = match matches.subcommand() {
        ("cf", Some(cf)) => {
            match cf.subcommand_name().unwrap() {
                "d" => cf_d_write(&db, &mut *keys_d, &mut *vals_d, batch_size, default_cf),
                "l" => cf_l_write(&db, &mut *keys_l, &mut *vals_l, batch_size, lock_cf),
                "w" => cf_w_write(&db, &mut *keys_w, &mut *vals_w, batch_size, write_cf),
                "r" => cf_r_write(&db, &mut *keys_r, &mut *vals_r, batch_size, raft_cf),
                "dl" => {
                    cf_dl_write(&db,
                                &mut *keys_d,
                                &mut *vals_d,
                                &mut *keys_l,
                                &mut *vals_l,
                                batch_size,
                                default_cf,
                                lock_cf)
                }
                "dr" => {
                    cf_dr_write(&db,
                                &mut *keys_d,
                                &mut *vals_d,
                                &mut *keys_r,
                                &mut *vals_r,
                                batch_size,
                                default_cf,
                                raft_cf)
                }
                "dw" => {
                    cf_dw_write(&db,
                                &mut *keys_d,
                                &mut *vals_d,
                                &mut *keys_w,
                                &mut *vals_w,
                                batch_size,
                                default_cf,
                                write_cf)
                }
                "lr" => {
                    cf_lr_write(&db,
                                &mut *keys_l,
                                &mut *vals_l,
                                &mut *keys_r,
                                &mut *vals_r,
                                batch_size,
                                lock_cf,
                                raft_cf)
                }
                "lw" => {
                    cf_lw_write(&db,
                                &mut *keys_l,
                                &mut *vals_l,
                                &mut *keys_w,
                                &mut *vals_w,
                                batch_size,
                                lock_cf,
                                write_cf)
                }
                "rw" => {
                    cf_rw_write(&db,
                                &mut *keys_r,
                                &mut *vals_r,
                                &mut *keys_w,
                                &mut *vals_w,
                                batch_size,
                                raft_cf,
                                write_cf)
                }
                "dlr" => {
                    cf_dlr_write(&db,
                                 &mut *keys_d,
                                 &mut *vals_d,
                                 &mut *keys_l,
                                 &mut *vals_l,
                                 &mut *keys_r,
                                 &mut *vals_r,
                                 batch_size,
                                 default_cf,
                                 lock_cf,
                                 raft_cf)
                }
                "dlw" => {
                    cf_dlw_write(&db,
                                 &mut *keys_d,
                                 &mut *vals_d,
                                 &mut *keys_l,
                                 &mut *vals_l,
                                 &mut *keys_w,
                                 &mut *vals_w,
                                 batch_size,
                                 default_cf,
                                 lock_cf,
                                 write_cf)
                }
                "drw" => {
                    cf_drw_write(&db,
                                 &mut *keys_d,
                                 &mut *vals_d,
                                 &mut *keys_r,
                                 &mut *vals_r,
                                 &mut *keys_w,
                                 &mut *vals_w,
                                 batch_size,
                                 default_cf,
                                 raft_cf,
                                 write_cf)
                }
                "lrw" => {
                    cf_lrw_write(&db,
                                 &mut *keys_l,
                                 &mut *vals_l,
                                 &mut *keys_r,
                                 &mut *vals_r,
                                 &mut *keys_w,
                                 &mut *vals_w,
                                 batch_size,
                                 lock_cf,
                                 raft_cf,
                                 write_cf)
                }
                "dlrw" => {
                    cf_dlrw_write(&db,
                                  &mut *keys_d,
                                  &mut *vals_d,
                                  &mut *keys_l,
                                  &mut *vals_l,
                                  &mut *keys_r,
                                  &mut *vals_r,
                                  &mut *keys_w,
                                  &mut *vals_w,
                                  batch_size,
                                  default_cf,
                                  lock_cf,
                                  raft_cf,
                                  write_cf)
                }
                _ => help_err(app),
            }
        }
        ("txn", _) => {
            return Err("txn bench mark not impl".to_owned());
        }
        _ => help_err(app),
    };

    output_stats(&db);

    match res {
        Ok(_) => Ok(count),
        Err(e) => Err(e),
    }
}

fn help_err(app: clap::App) -> Result<(), String> {
    let mut help = Vec::new();
    app.write_help(&mut help).unwrap();
    Err(String::from_utf8(help).unwrap())
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
