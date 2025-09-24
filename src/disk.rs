use anyhow::Result;
use std::env;
use std::path::PathBuf;
use sysinfo::{DiskExt, System, SystemExt};

/// Trait to abstract over real vs mocked disk providers.
pub trait DiskProvider {
    fn refresh(&mut self);
    fn disks(&self) -> Vec<(u64, u64, PathBuf)>;
}

/// Real provider that uses `sysinfo`.
pub struct SysDiskProvider {
    sys: System,
}

impl SysDiskProvider {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_disks_list();
        sys.refresh_disks();
        Self { sys }
    }
}

impl DiskProvider for SysDiskProvider {
    fn refresh(&mut self) {
        self.sys.refresh_disks_list();
        self.sys.refresh_disks();
    }

    fn disks(&self) -> Vec<(u64, u64, PathBuf)> {
        self.sys
            .disks()
            .iter()
            .map(|d| {
                (
                    d.available_space(),
                    d.total_space(),
                    d.mount_point().to_path_buf(),
                )
            })
            .collect()
    }
}

pub fn disk_usage_with<P: DiskProvider>(provider: &mut P) -> Result<(Vec<(u64, u64)>, f64)> {
    provider.refresh();
    let disks = provider.disks();

    let result: Vec<(u64, u64)> = disks.iter().map(|(a, t, _)| (*a, *t)).collect();

    let cwd = env::current_dir()?; // no canonicalize

    for (avail, total, mount) in disks {
        if total > 0 && cwd.starts_with(&mount) {
            return Ok((result, (avail as f64 / total as f64) * 100.0));
        }
    }

    Ok((result, 404.0))
}

pub fn disk_usage() -> Result<(Vec<(u64, u64)>, f64)> {
    let mut provider = SysDiskProvider::new();
    disk_usage_with(&mut provider)
}

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    struct MockDiskProvider {
        disks: Vec<(u64, u64, PathBuf)>,
    }

    impl DiskProvider for MockDiskProvider {
        fn refresh(&mut self) {}
        fn disks(&self) -> Vec<(u64, u64, PathBuf)> {
            self.disks.clone()
        }
    }

    #[serial_test::serial]
    #[test]
    fn cwd_not_in_any_disk_returns_404() {
        let tmp = tempdir().unwrap();
        env::set_current_dir(tmp.path()).unwrap();

        let mut mock = MockDiskProvider {
            // unrelated mount (canonicalized for macOS safety)
            disks: vec![(
                100,
                200,
                PathBuf::from("/unrelated/mount")
                    .canonicalize()
                    .unwrap_or_else(|_| PathBuf::from("/unrelated/mount")),
            )],
        };

        let (_disks, usage) = disk_usage_with(&mut mock).unwrap();
        assert_eq!(usage, 404.0);
    }

    #[serial_test::serial]
    #[test]
    fn cwd_in_disk_mount_returns_percentage() {
        let tmp = tempdir().unwrap();
        let mount_point = tmp.path().canonicalize().unwrap();

        let cwd = mount_point.join("subdir");
        std::fs::create_dir_all(&cwd).unwrap();
        env::set_current_dir(&cwd).unwrap();

        let mut mock = MockDiskProvider {
            disks: vec![(50, 100, mount_point.clone())],
        };

        // Debug info if it fails on CI
        let cwd_actual = env::current_dir().unwrap();
        eprintln!("mount_point = {:?}", mount_point);
        eprintln!("cwd_actual   = {:?}", cwd_actual);

        let (_disks, usage) = disk_usage_with(&mut mock).unwrap();
        assert_eq!(
            usage, 50.0,
            "cwd ({:?}) did not match mount ({:?})",
            cwd_actual, mount_point
        );
    }

    #[serial_test::serial]
    #[test]
    fn result_contains_all_disks() {
        let tmp = tempdir().unwrap();
        let mount_point = tmp.path().canonicalize().unwrap();

        let cwd = mount_point.join("zero");
        std::fs::create_dir_all(&cwd).unwrap();
        env::set_current_dir(&cwd).unwrap();

        let mut mock = MockDiskProvider {
            disks: vec![
                (50, 100, mount_point.clone()),
                (
                    30,
                    60,
                    PathBuf::from("/other/mount")
                        .canonicalize()
                        .unwrap_or_else(|_| PathBuf::from("/other/mount")),
                ),
                (
                    10,
                    20,
                    PathBuf::from("/yet/another/mount")
                        .canonicalize()
                        .unwrap_or_else(|_| PathBuf::from("/yet/another/mount")),
                ),
            ],
        };

        let (disks, usage) = disk_usage_with(&mut mock).unwrap();
        assert_eq!(disks, vec![(50, 100), (30, 60), (10, 20)]);
        assert_eq!(usage, 50.0);
    }

    #[serial_test::serial]
    #[test]
    fn does_not_panic_if_total_space_is_zero() {
        let tmp = tempdir().unwrap();
        let mount_point = tmp.path().canonicalize().unwrap();

        let cwd = mount_point.join("zero");
        std::fs::create_dir_all(&cwd).unwrap();
        env::set_current_dir(&cwd).unwrap();

        let mut mock = MockDiskProvider {
            disks: vec![(0, 0, mount_point.clone())], // total = 0
        };

        let (_disks, usage) = disk_usage_with(&mut mock).unwrap();
        assert_eq!(usage, 404.0);
    }

    #[serial_test::serial]
    #[test]
    fn returns_disks_and_usage_in_valid_range() {
        let _tmp = tempdir().unwrap(); // keep alive
        let (disks, usage) = disk_usage().unwrap();
        assert!(!disks.is_empty());

        for (avail, total) in &disks {
            assert!(*avail <= *total);
        }

        assert!(
            (0.0..=100.0).contains(&usage) || (usage - 404.0).abs() < f64::EPSILON,
            "usage was {usage}"
        );
    }

    #[serial_test::serial]
    #[test]
    fn cwd_random_directory() {
        let tmp = tempdir().unwrap();
        env::set_current_dir(tmp.path()).unwrap();

        let (_disks, usage) = disk_usage().unwrap();

        assert!(
            (0.0..=100.0).contains(&usage) || (usage - 404.0).abs() < f64::EPSILON,
            "usage was {usage}"
        );
    }
}
