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

use std::process;
use std::time::Instant;
use clap::{Arg, App, SubCommand};
use rocksdb::DB;

mod sim;
mod env;
use env::dbcfg;
use sim::{key, val};

fn run() -> Result<usize, String> {
    let app = App::new("Rocksdb in TiKV")
        .author("PingCAP")
        .about("Benchmark of rocksdb in the sim-tikv-env")
        .arg(Arg::with_name("db")
            .short("db")
            .takes_value(true)
            .help("rocksdb path")
            .required(true))
        .arg(Arg::with_name("cfg")
            .short("c")
            .takes_value(true)
            .help("toml config file")
            .required(true))
        .arg(Arg::with_name("count")
            .short("n")
            .takes_value(true)
            .help("request count")
            .required(true))
        .arg(Arg::with_name("nosyscheck")
            .takes_value(false)
            .help("skip system check")
            .required(false))
        .subcommand(SubCommand::with_name("cf")
            .subcommand(SubCommand::with_name("data"))
            .subcommand(SubCommand::with_name("lock"))
            .subcommand(SubCommand::with_name("commit"))
            .subcommand(SubCommand::with_name("raft")))
        .subcommand(SubCommand::with_name("txn"));

    let matches = app.clone().get_matches();

    if !matches.is_present("nosyscheck") {
        if let Err(e) = env::check::check_system_config() {
            return Err(format!("system config not satisfied: {}\n", e));
        }
    }

    let db_path = matches.value_of("db").unwrap();
    let cfg = matches.value_of("cfg").unwrap();
    let (opt_db, opt_cf) = try!(dbcfg::get_db_config(cfg));
    let db = try!(DB::open_cf(opt_db, db_path, &["default"], &[&opt_cf]));

    let count = matches.value_of("count").unwrap();
    let count: usize = match count.parse() {
        Ok(v) => v,
        Err(_) => return Err("-n <count>: is not a number".to_owned()),
    };

    let res = match matches.subcommand() {
        ("cf", Some(cf)) => {
            match cf.subcommand_name() {
                Some("data") => sim::cf::cf_default_w(db),
                Some("lock") => sim::cf::cf_lock_w(db,
                    &mut key::IncreaseKeyGen::new(&vec![0; 32], count),
                    &mut val::ConstValGen::new(b"test-val")),
                Some("commit") => sim::cf::cf_write_w(db),
                Some("raft") => sim::cf::cf_raft_w(db),
                _ => help_err(app)
            }
        }
        ("txn", Some(_)) => {
            return Err("txn bench mark not impl".to_owned());
        }
        _ => help_err(app)
    };

    match res {
        Ok(_) => Ok(count),
        Err(e) => Err(e)
    }
}

fn help_err(app: clap::App) -> Result<(), String> {
    let mut help = Vec::new();
    app.write_help(&mut help).unwrap();
    Err(String::from_utf8(help).unwrap())
}

fn main() {
    let timer = Instant::now();
    match run() {
        Err(e) => {
            print!("{}\n", e);
            process::exit(1)
        }
        Ok(count) => {
            let elapsed = timer.elapsed();
            let tps = count as f64 / (elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9);
            print!("invoke {} times in {} ms, tps: {}\n",
                count,
                elapsed.as_secs() * 1000 + (elapsed.subsec_nanos() as f64 / 1e6) as u64,
                tps as u64);
        }
    };
}
