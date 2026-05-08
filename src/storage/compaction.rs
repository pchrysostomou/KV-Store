#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionConfig {
    pub level0_file_trigger: usize,
    pub max_levels: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            level0_file_trigger: 4,
            max_levels: 7,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionReport {
    pub flushed_memtable: bool,
    pub input_sstables: usize,
    pub output_sstables: usize,
    pub input_entries: u64,
    pub output_entries: u64,
    pub dropped_entries: u64,
}

impl CompactionReport {
    pub fn empty() -> Self {
        Self {
            flushed_memtable: false,
            input_sstables: 0,
            output_sstables: 0,
            input_entries: 0,
            output_entries: 0,
            dropped_entries: 0,
        }
    }
}
