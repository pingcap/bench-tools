// Copyright 2016 PingCAP, Inc.
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

use toml;
use process;

const UNIT: usize = 1;
const DATA_MAGNITUDE: usize = 1024;
const KB: usize = UNIT * DATA_MAGNITUDE;
const MB: usize = KB * DATA_MAGNITUDE;
const GB: usize = MB * DATA_MAGNITUDE;

const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

const TIME_MAGNITUDE_1: usize = 1000;
const TIME_MAGNITUDE_2: usize = 60;
const MS: usize = UNIT;
const SECOND: usize = MS * TIME_MAGNITUDE_1;
const MINTUE: usize = SECOND * TIME_MAGNITUDE_2;
const HOUR: usize = MINTUE * TIME_MAGNITUDE_2;

// TODO: remove this, do not exit. read with error return
fn exit_with_err(msg: String) -> ! {
    print!("{}", msg);
    process::exit(1)
}

fn split_property(property: &str) -> Result<(f64, &str), String> {
    let mut indx = 0;
    for s in property.chars() {
        match s {
            '0'...'9' | '.' => {
                indx += 1;
            }
            _ => {
                break;
            }
        }
    }

    let (num, unit) = property.split_at(indx);
    num.parse::<f64>().map(|f| (f, unit)).or(Err("bad format".to_owned()))
}

pub fn parse_readable_int(size: &str) -> Result<i64, String> {
    let (num, unit) = try!(split_property(size));

    match &*unit.to_lowercase() {
        // file size
        "kb" => Ok((num * (KB as f64)) as i64),
        "mb" => Ok((num * (MB as f64)) as i64),
        "gb" => Ok((num * (GB as f64)) as i64),
        "tb" => Ok((num * (TB as f64)) as i64),
        "pb" => Ok((num * (PB as f64)) as i64),

        // time
        "ms" => Ok((num * (MS as f64)) as i64),
        "s" => Ok((num * (SECOND as f64)) as i64),
        "m" => Ok((num * (MINTUE as f64)) as i64),
        "h" => Ok((num * (HOUR as f64)) as i64),

        _ => Err(format!("not a number: {}", unit)),
    }
}

pub fn get_toml_boolean(config: &toml::Value, name: &str, default: Option<bool>) -> bool {
    let b = match config.lookup(name) {
        Some(&toml::Value::Boolean(b)) => b,
        None => {
            default.unwrap_or_else(|| exit_with_err(format!("please specify {}", name)))
        }
        _ => exit_with_err(format!("{} boolean is excepted", name)),
    };

    b
}

pub fn get_toml_string(config: &toml::Value, name: &str, default: Option<String>) -> String {
    let s = match config.lookup(name) {
        Some(&toml::Value::String(ref s)) => s.clone(),
        None => {
            default.unwrap_or_else(|| exit_with_err(format!("please specify {}", name)))
        }
        _ => exit_with_err(format!("{} string is excepted", name)),
    };
    s
}

pub fn get_toml_int_opt(config: &toml::Value, name: &str) -> Option<i64> {
    let res = match config.lookup(name) {
        Some(&toml::Value::Integer(i)) => Some(i),
        Some(&toml::Value::String(ref s)) => {
            Some(parse_readable_int(s)
                .unwrap_or_else(|e| exit_with_err(format!("{} parse failed {:?}", name, e))))
        }
        None => None,
        _ => exit_with_err(format!("{} int or readable int is excepted", name)),
    };
    res
}

pub fn get_toml_int(config: &toml::Value, name: &str, default: Option<i64>) -> i64 {
    get_toml_int_opt(config, name).unwrap_or_else(|| {
        let i = default.unwrap_or_else(|| exit_with_err(format!("please specify {}", name)));
        i
    })
}
