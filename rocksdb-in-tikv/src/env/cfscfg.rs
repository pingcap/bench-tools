extern crate byteorder;

use rocksdb::{DB, Options as RocksdbOptions, BlockBasedOptions, SliceTransform};
use rocksdb::{DBCompressionType, DBRecoveryMode};
use rocksdb::rocksdb_options::Options;
use toml;
use std::process;
use std::path::Path;
use std::fs;
use std::mem;
use super::helper::{get_toml_boolean, get_toml_int, get_toml_string};
use env::CF_DEFAULT;

const SEC_TO_MS: i64 = 1000;
const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
const KB: u64 = UNIT * DATA_MAGNITUDE;
const MB: u64 = KB * DATA_MAGNITUDE;
const GB: u64 = MB * DATA_MAGNITUDE;
const RAFTCF_MIN_MEM: u64 = 256 * MB;
const RAFTCF_MAX_MEM: u64 = 2 * GB;
const LOCKCF_MIN_MEM: u64 = 256 * MB;
const LOCKCF_MAX_MEM: u64 = GB;

pub const LOCAL_PREFIX: u8 = 0x01;
pub const REGION_RAFT_PREFIX: u8 = 0x02;
pub const REGION_RAFT_PREFIX_KEY: &'static [u8] = &[LOCAL_PREFIX, REGION_RAFT_PREFIX];

pub fn exit_with_err(msg: String) -> ! {
    error!("{}", msg);
    process::exit(1)
}

pub fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter().filter(|x| b.iter().find(|y| y == x).is_none()).map(|x| *x).collect()
}

pub fn region_raft_prefix_len() -> usize {
    // REGION_RAFT_PREFIX_KEY + region_id + suffix
    REGION_RAFT_PREFIX_KEY.len() + mem::size_of::<u64>() + 1
}

fn align_to_mb(n: u64) -> u64 {
    n & 0xFFFFFFFFFFF00000
}

fn adjust_block_cache_size(cache_size: u64, min_limit: u64, max_limit: u64) -> u64 {
    if cache_size < min_limit {
        return min_limit;
    }
    if cache_size > max_limit {
        return max_limit;
    }
    cache_size
}

pub struct CFOptions<'a> {
    cf: &'a str,
    options: Options,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: Options) -> CFOptions<'a> {
        CFOptions {
            cf: cf,
            options: options,
        }
    }
}

struct CfOptValues {
    pub block_size: i64,
    pub block_cache_size: i64,
    pub cache_index_and_filter_blocks: bool,
    pub use_bloom_filter: bool,
    pub whole_key_filtering: bool,
    pub bloom_bits_per_key: i64,
    pub block_based_filter: bool,
    pub compression_per_level: String,
    pub write_buffer_size: i64,
    pub max_write_buffer_number: i64,
    pub min_write_buffer_number_to_merge: i64,
    pub max_bytes_for_level_base: i64,
    pub target_file_size_base: i64,
    pub level_zero_file_num_compaction_trigger: i64,
    pub level_zero_slowdown_writes_trigger: i64,
    pub level_zero_stop_writes_trigger: i64,
    pub compaction_priority: i64,
}

// TODO: verify: (TiDB default values) == (rocksdb default values)
impl Default for CfOptValues {
    fn default() -> CfOptValues {
        CfOptValues {
            block_size: 64 * KB as i64,
            block_cache_size: 256 * MB as i64,
            cache_index_and_filter_blocks: true,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_bits_per_key: 10,
            block_based_filter: false,
            compression_per_level: String::from("no:no:lz4:lz4:lz4:lz4:lz4"),
            write_buffer_size: 128 * MB as i64,
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: 512 * MB as i64,
            target_file_size_base: 32 * MB as i64,
            level_zero_file_num_compaction_trigger: 4,
            level_zero_slowdown_writes_trigger: 20,
            level_zero_stop_writes_trigger: 36,
            compaction_priority: 0,
        }
    }
}

fn check_and_open(path: &str, mut db_opt: Options, cfs_opts: Vec<CFOptions>) -> Result<DB, String> {
    // If db not exist, create it.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut cfs = vec![];
        let mut cfs_opts_ref = vec![];
        if let Some(x) = cfs_opts.iter().find(|x| x.cf == CF_DEFAULT) {
            cfs.push(CF_DEFAULT);
            cfs_opts_ref.push(&x.options);
        }
        let mut db = try!(DB::open_cf(db_opt, path, cfs.as_slice(), cfs_opts_ref.as_slice()));
        for x in &cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            try!(db.create_cf(x.cf, &x.options));
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // List all column families in current db.
    let cfs_list = try!(DB::list_column_families(&db_opt, path));
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cfs_opts.iter().map(|x| x.cf).collect();
    // If all column families are exist, just open db.
    if existed == needed {
        let mut cfs = vec![];
        let mut cfs_opts_ref = vec![];
        for x in &cfs_opts {
            cfs.push(x.cf);
            cfs_opts_ref.push(&x.options);
        }

        return DB::open_cf(db_opt, path, cfs.as_slice(), cfs_opts_ref.as_slice());
    }

    // Open db.
    let common_opt = Options::new();
    let mut cfs = vec![];
    let mut cfs_opts_ref = vec![];
    for cf in &existed {
        cfs.push(*cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                cfs_opts_ref.push(&x.options);
            }
            None => {
                cfs_opts_ref.push(&common_opt);
            }
        }
    }
    let mut db = DB::open_cf(db_opt, path, cfs.as_slice(), cfs_opts_ref.as_slice()).unwrap();

    // Drop discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            try!(db.drop_cf(cf));
        }
    }

    // Create needed column families not existed yet.
    for cf in cfs_diff(&needed, &existed) {
        try!(db.create_cf(cf, &cfs_opts.iter().find(|x| x.cf == cf).unwrap().options));
    }

    Ok(db)
}

pub fn new_engine_opt(path: &str, opts: Options, cfs_opts: Vec<CFOptions>) -> Result<DB, String> {
    check_and_open(path, opts, cfs_opts)
}

fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, DB::list_column_families will failed and we can cleanup
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len: suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len: prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub fn get_rocksdb_db_option(config: &toml::Value) -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let rmode = get_toml_int(config, "rocksdb.wal-recovery-mode", Some(2));
    let wal_recovery_mode = parse_rocksdb_wal_recovery_mode(rmode)
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
    opts.set_wal_recovery_mode(wal_recovery_mode);

    let wal_dir = get_toml_string(config, "rocksdb.wal-dir", Some("".to_owned()));
    if !wal_dir.is_empty() {
        opts.set_wal_dir(&wal_dir)
    };

    let wal_ttl_seconds = get_toml_int(config, "rocksdb.wal-ttl-seconds", Some(0));
    opts.set_wal_ttl_seconds(wal_ttl_seconds as u64);

    let wal_size_limit = get_toml_int(config, "rocksdb.wal-size-limit", Some(0));
    // return size in MB
    let wal_size_limit_mb = align_to_mb(wal_size_limit as u64) / MB;
    opts.set_wal_size_limit_mb(wal_size_limit_mb as u64);

    let max_total_wal_size = get_toml_int(config,
                                          "rocksdb.max-total-wal-size",
                                          Some(4 * 1024 * 1024 * 1024));
    opts.set_max_total_wal_size(max_total_wal_size as u64);

    let max_background_compactions =
        get_toml_int(config, "rocksdb.max-background-compactions", Some(6));
    opts.set_max_background_compactions(max_background_compactions as i32);

    let max_background_flushes = get_toml_int(config, "rocksdb.max-background-flushes", Some(2));
    opts.set_max_background_flushes(max_background_flushes as i32);

    let base_bg_compactions = get_toml_int(config, "rocksdb.base-background-compactions", Some(1));
    opts.set_base_background_compactions(base_bg_compactions as i32);

    let max_manifest_file_size = get_toml_int(config,
                                              "rocksdb.max-manifest-file-size",
                                              Some(20 * 1024 * 1024));
    opts.set_max_manifest_file_size(max_manifest_file_size as u64);

    let create_if_missing = get_toml_boolean(config, "rocksdb.create-if-missing", Some(true));
    opts.create_if_missing(create_if_missing);

    let max_open_files = get_toml_int(config, "rocksdb.max-open-files", Some(40960));
    opts.set_max_open_files(max_open_files as i32);

    let enable_statistics = get_toml_boolean(config, "rocksdb.enable-statistics", Some(true));
    if enable_statistics {
        opts.enable_statistics();
        let stats_dump_period_sec =
            get_toml_int(config, "rocksdb.stats-dump-period-sec", Some(600));
        opts.set_stats_dump_period_sec(stats_dump_period_sec as usize);
    }

    let compaction_readahead_size =
        get_toml_int(config, "rocksdb.compaction-readahead-size", Some(0));
    opts.set_compaction_readahead_size(compaction_readahead_size as u64);

    let max_file_size = get_toml_int(config, "rocksdb.info-log-max-size", Some(0));
    opts.set_max_log_file_size(max_file_size as u64);

    // RocksDB needs seconds, but here we will get milliseconds.
    let roll_time_secs = get_toml_int(config, "rocksdb.info-log-roll-time", Some(0)) / SEC_TO_MS;
    opts.set_log_file_time_to_roll(roll_time_secs as u64);

    let info_log_dir = get_toml_string(config, "rocksdb.info-log-dir", Some("".to_owned()));
    if !info_log_dir.is_empty() {
        opts.create_info_log(&info_log_dir).unwrap_or_else(|e| {
            panic!("create RocksDB info log {} error {:?}", info_log_dir, e);
        })
    }

    let rate_bytes_per_sec = get_toml_int(config, "rocksdb.rate-bytes-per-sec", Some(0));
    if rate_bytes_per_sec > 0 {
        opts.set_ratelimiter(rate_bytes_per_sec as i64);
    }

    let max_sub_compactions = get_toml_int(config, "rocksdb.max-sub-compactions", Some(1));
    opts.set_max_subcompactions(max_sub_compactions as u32);

    let writable_file_max_buffer_size = get_toml_int(config,
                                                     "rocksdb.writable-file-max-buffer-size",
                                                     Some(1024 * 1024));
    opts.set_writable_file_max_buffer_size(writable_file_max_buffer_size as i32);

    let direct_io = get_toml_boolean(config,
                                     "rocksdb.use-direct-io-for-flush-and-compaction",
                                     Some(false));
    opts.set_use_direct_io_for_flush_and_compaction(direct_io);

    opts
}

fn get_rocksdb_cf_option(config: &toml::Value,
                         cf: &str,
                         default_values: CfOptValues)
                         -> RocksdbOptions {
    let prefix = String::from("rocksdb.") + cf + ".";
    let mut block_base_opts = BlockBasedOptions::new();
    let block_size = get_toml_int(config,
                                  (prefix.clone() + "block-size").as_str(),
                                  Some(default_values.block_size));
    block_base_opts.set_block_size(block_size as usize);
    let block_cache_size = get_toml_int(config,
                                        (prefix.clone() + "block-cache-size").as_str(),
                                        Some(default_values.block_cache_size));
    block_base_opts.set_lru_cache(block_cache_size as usize);

    let cache_index_and_filter =
        get_toml_boolean(config,
                         (prefix.clone() + "cache-index-and-filter-blocks").as_str(),
                         Some(default_values.cache_index_and_filter_blocks));
    block_base_opts.set_cache_index_and_filter_blocks(cache_index_and_filter);

    if default_values.use_bloom_filter {
        let bloom_bits_per_key = get_toml_int(config,
                                              (prefix.clone() + "bloom-filter-bits-per-key")
                                                  .as_str(),
                                              Some(default_values.bloom_bits_per_key));
        let block_based_filter = get_toml_boolean(config,
                                                  (prefix.clone() + "block-based-bloom-filter")
                                                      .as_str(),
                                                  Some(default_values.block_based_filter));
        block_base_opts.set_bloom_filter(bloom_bits_per_key as i32, block_based_filter);

        block_base_opts.set_whole_key_filtering(default_values.whole_key_filtering);
    }
    let mut opts = RocksdbOptions::new();
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = get_toml_string(config,
                              (prefix.clone() + "compression-per-level").as_str(),
                              Some(default_values.compression_per_level.clone()));
    let per_level_compression = parse_rocksdb_per_level_compression(&cpl)
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
    opts.compression_per_level(&per_level_compression);

    let write_buffer_size = get_toml_int(config,
                                         (prefix.clone() + "write-buffer-size").as_str(),
                                         Some(default_values.write_buffer_size));
    opts.set_write_buffer_size(write_buffer_size as u64);

    let max_write_buffer_number = get_toml_int(config,
                                               (prefix.clone() + "max-write-buffer-number")
                                                   .as_str(),
                                               Some(default_values.max_write_buffer_number));
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);

    let min_write_buffer_number_to_merge =
        get_toml_int(config,
                     (prefix.clone() + "min-write-buffer-number-to-merge").as_str(),
                     Some(default_values.min_write_buffer_number_to_merge));
    opts.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge as i32);

    let max_bytes_for_level_base = get_toml_int(config,
                                                (prefix.clone() + "max-bytes-for-level-base")
                                                    .as_str(),
                                                Some(default_values.max_bytes_for_level_base));
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);

    let target_file_size_base = get_toml_int(config,
                                             (prefix.clone() + "target-file-size-base").as_str(),
                                             Some(default_values.target_file_size_base));
    opts.set_target_file_size_base(target_file_size_base as u64);

    let level_zero_file_num_compaction_trigger =
        get_toml_int(config,
                     (prefix.clone() + "level0-file-num-compaction-trigger").as_str(),
                     Some(default_values.level_zero_file_num_compaction_trigger));
    opts.set_level_zero_file_num_compaction_trigger(level_zero_file_num_compaction_trigger as i32);

    let level_zero_slowdown_writes_trigger =
        get_toml_int(config,
                     (prefix.clone() + "level0-slowdown-writes-trigger").as_str(),
                     Some(default_values.level_zero_slowdown_writes_trigger));
    opts.set_level_zero_slowdown_writes_trigger(level_zero_slowdown_writes_trigger as i32);

    let level_zero_stop_writes_trigger =
        get_toml_int(config,
                     (prefix.clone() + "level0-stop-writes-trigger").as_str(),
                     Some(default_values.level_zero_stop_writes_trigger));
    opts.set_level_zero_stop_writes_trigger(level_zero_stop_writes_trigger as i32);

    opts
}

pub fn get_rocksdb_default_cf_option(config: &toml::Value) -> RocksdbOptions {
    let mut default_values = CfOptValues::default();
    // default_values.block_cache_size =
    //     align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[0]) as u64) as i64;
    default_values.use_bloom_filter = true;
    default_values.whole_key_filtering = true;

    get_rocksdb_cf_option(config, "defaultcf", default_values)
}

pub fn get_rocksdb_write_cf_option(config: &toml::Value) -> RocksdbOptions {
    let mut default_values = CfOptValues::default();
    // default_values.block_cache_size =
    // align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[1]) as u64) as i64;
    default_values.use_bloom_filter = true;
    default_values.whole_key_filtering = false;

    let mut opts = get_rocksdb_cf_option(config, "writecf", default_values);
    // Prefix extractor(trim the timestamp at tail) for write cf.
    opts.set_prefix_extractor("FixedSuffixSliceTransform",
                              Box::new(FixedSuffixSliceTransform::new(8)))
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
    // Create prefix bloom filter for memtable.
    opts.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
    opts
}

pub fn get_rocksdb_raftlog_cf_option(config: &toml::Value) -> RocksdbOptions {
    // let cache_size = align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[2]) as u64);
    let block_cache_size = adjust_block_cache_size(134217728, RAFTCF_MIN_MEM, RAFTCF_MAX_MEM);
    let mut default_values = CfOptValues::default();
    default_values.block_cache_size = block_cache_size as i64;

    let mut opts = get_rocksdb_cf_option(config, "raftcf", default_values);
    opts.set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform",
            Box::new(FixedPrefixSliceTransform::new(region_raft_prefix_len())))
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
    opts
}

pub fn get_rocksdb_lock_cf_option(config: &toml::Value) -> RocksdbOptions {
    // let cache_size = align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[3]) as u64);
    let block_cache_size = adjust_block_cache_size(134217728, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM);
    let mut default_values = CfOptValues::default();
    default_values.block_cache_size = block_cache_size as i64;
    default_values.block_size = 16 * KB as i64;
    default_values.use_bloom_filter = true;
    default_values.whole_key_filtering = true;
    default_values.compression_per_level = String::from("no:no:no:no:no:no:no");
    default_values.level_zero_file_num_compaction_trigger = 1;
    default_values.max_bytes_for_level_base = 128 * MB as i64;

    let mut opts = get_rocksdb_cf_option(config, "lockcf", default_values);
    // Currently if we want create bloom filter for memtable, we must set prefix extractor.
    opts.set_prefix_extractor("NoopSliceTransform", Box::new(NoopSliceTransform))
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
    opts.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
    opts
}

pub fn parse_rocksdb_per_level_compression(tp: &str)
                                           -> Result<Vec<DBCompressionType>, &'static str> {
    let mut result: Vec<DBCompressionType> = vec![];
    let v: Vec<&str> = tp.split(':').collect();
    for i in &v {
        match &*i.to_lowercase() {
            "no" => result.push(DBCompressionType::DBNo),
            "snappy" => result.push(DBCompressionType::DBSnappy),
            "zlib" => result.push(DBCompressionType::DBZlib),
            "bzip2" => result.push(DBCompressionType::DBBz2),
            "lz4" => result.push(DBCompressionType::DBLz4),
            "lz4hc" => result.push(DBCompressionType::DBLz4hc),
            "zstd" => result.push(DBCompressionType::DBZstd),
            _ => return Err("Error"),
        }
    }

    Ok(result)
}

pub fn parse_rocksdb_wal_recovery_mode(mode: i64) -> Result<DBRecoveryMode, &'static str> {
    match mode {
        0 => Ok(DBRecoveryMode::TolerateCorruptedTailRecords),
        1 => Ok(DBRecoveryMode::AbsoluteConsistency),
        2 => Ok(DBRecoveryMode::PointInTime),
        3 => Ok(DBRecoveryMode::SkipAnyCorruptedRecords),
        _ => Err("Error"),
    }
}
