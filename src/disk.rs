use anyhow::Result;
use std::env;
use sysinfo::{DiskExt, System, SystemExt};

pub fn disk_usage() -> Result<(Vec<(u64, u64)>, f64)> {
    let mut sys = System::new_all();
    sys.refresh_disks_list();
    sys.refresh_disks();

    let mut result: Vec<(u64, u64)> = vec![];

    for disk in sys.disks() {
        result.push((disk.available_space(), disk.total_space()));
    }

    let cwd = env::current_dir().expect("Failed to get cwd");

    for disk in sys.disks() {
        let mount_point = disk.mount_point();
        if cwd.starts_with(mount_point) {
            return Ok((
                result,
                (disk.available_space() as f64 / disk.total_space() as f64) * 100.0,
            ));
        }
    }

    return Ok((result, 404.0));
}
