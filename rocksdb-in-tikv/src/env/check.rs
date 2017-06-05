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

use std::env;

#[cfg(unix)]
pub fn check_max_open_fds(expect: u64) -> Result<(), String> {
    use std::mem;
    use libc;

    unsafe {
        let mut fd_limit = mem::zeroed();
        let mut err = libc::getrlimit(libc::RLIMIT_NOFILE, &mut fd_limit);
        if err != 0 {
            return Err("check_max_open_fds failed".to_owned());
        }
        if fd_limit.rlim_cur >= expect {
            return Ok(());
        }

        let prev_limit = fd_limit.rlim_cur;
        fd_limit.rlim_cur = expect;
        if fd_limit.rlim_max < expect {
            // If the process is not started by privileged user, this will fail.
            fd_limit.rlim_max = expect;
        }
        err = libc::setrlimit(libc::RLIMIT_NOFILE, &fd_limit);
        if err == 0 {
            return Ok(());
        }
        Err(format!("open files' limit is too small, got {}, expect >= {}",
            prev_limit, expect))
    }
}

#[cfg(not(unix))]
pub fn check_max_open_fds(_: u64) -> Result<(), String> {
    Ok(())
}

#[cfg(target_os = "linux")]
mod check_kernel {
    use std::fs;
    use std::io::Read;

    type Checker = Fn(i64, i64) -> bool;

    fn check_kernel_params(param_path: &str,
        expect: i64, checker: Box<Checker>) -> Result<(), String> {

        let mut buffer = String::new();
        if let Err(e) = fs::File::open(param_path).and_then(|mut f| f.read_to_string(&mut buffer)) {
            return Err(format!("open path failed while checking kernel params: {}, {}", param_path, e));
        }

        let got = buffer.trim_matches('\n').parse::<i64>();
        if let Err(e) = got {
            return Err(format!("pasrse params failed while checking kernel params: {}, {}", param_path, e));
        }
        let got = got.unwrap();

        let mut param = String::new();
        // skip 3, ["", "proc", "sys", ...]
        for path in param_path.split('/').skip(3) {
            param.push_str(path);
            param.push('.');
        }
        param.pop();

        if !checker(got, expect) {
            return Err(format!("kernel parameters {} got {}, expect {}", param, got, expect));
        }

        Ok(())
    }

    pub fn check_kernel() -> Vec<Result<(), String>> {
        let params: Vec<(&str, i64, Box<Checker>)> = vec![
            // Check vm.swappiness.
            ("/proc/sys/vm/swappiness", 0, Box::new(|got, expect| got == expect)),
        ];

        let mut errors = Vec::with_capacity(params.len());
        for (param_path, expect, checker) in params {
            if let Err(e) = check_kernel_params(param_path, expect, checker) {
                errors.push(Err(e));
            }
        }

        errors
    }
}

#[cfg(target_os = "linux")]
pub use self::check_kernel::check_kernel;

#[cfg(not(target_os = "linux"))]
pub fn check_kernel() -> Vec<Result<(), String>> {
    Vec::new()
}

pub fn check_system_config() -> Result<(), String> {
    if let Err(e) = check_max_open_fds(4096 as u64) {
        return Err(format!("{:?}", e));
    }

    // TODO: better iterater
    for e in check_kernel() {
        if let Err(e) = e {
            return Err(format!("{:?}", e));
        }
    }

    if !cfg!(windows) && env::var("TZ").is_err() {
        return Err("environment variable `TZ` is missing".to_owned());
    }
    Ok(())
}
