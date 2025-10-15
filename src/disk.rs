use std::env;
use std::path::PathBuf;
use sysinfo::{Disks, System};

/// Trait to abstract over real vs mocked disk providers.
pub trait DiskProvider {
    fn refresh(&mut self);
    fn disks(&self) -> Vec<(u64, u64, PathBuf)>;
}

/// Real provider that uses `sysinfo`.
pub struct SysDiskProvider {
    #[allow(dead_code)]
    sys: System,
    disks: Disks,
}

impl SysDiskProvider {
    pub fn new() -> Self {
        let sys = System::new(); // no automatic refresh
        let disks = Disks::new_with_refreshed_list(); // initializes and refreshes disks
        Self { sys, disks }
    }
}

impl DiskProvider for SysDiskProvider {
    fn refresh(&mut self) {
        self.disks.refresh(true); // remove_not_listed_disks = true
    }

    fn disks(&self) -> Vec<(u64, u64, PathBuf)> {
        self.disks
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

/// Best-effort "~" expansion without extra deps.
fn expand_tilde(p: &str) -> PathBuf {
    if p == "~" || p.starts_with("~/") {
        if let Some(home) = home_dir() {
            let tail = p.trim_start_matches("~/");
            return home.join(tail);
        }
    }
    PathBuf::from(p)
}

/// Try to find a home directory on Unix/Windows without pulling in `dirs`.
fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
}

/// Resolve the IPFS repository path:
/// 1) $IPFS_PATH if set (with "~" expansion)
/// 2) ~/.ipfs otherwise
fn resolve_ipfs_repo_path() -> Option<PathBuf> {
    if let Ok(ipfs_path) = env::var("IPFS_PATH") {
        return Some(expand_tilde(&ipfs_path));
    }
    let home = home_dir()?;
    Some(home.join(".ipfs"))
}

/// Returns:
/// - Vec with exactly one (available, total) for the disk containing the IPFS repo.
/// - Percentage available (0..=100) for that disk. Returns 404.0 if it can't be determined.
pub fn disk_usage_with<P: DiskProvider>(provider: &mut P) -> (Vec<(u64, u64)>, f64) {
    provider.refresh();
    let disks = provider.disks();

    // Resolve IPFS repo path
    let ipfs_repo = match resolve_ipfs_repo_path() {
        Some(p) => p,
        None => return (Vec::new(), 404.0),
    };

    // Canonicalize the repo path so mount-point matching is reliable.
    let repo_canon = match ipfs_repo.canonicalize() {
        Ok(p) => p,
        Err(_) => return (Vec::new(), 404.0), // repo path doesn't exist or can't be resolved
    };

    for (avail, total, mount) in disks {
        if total == 0 {
            continue; // guard against bogus entries
        }

        let mount_cmp = match mount.canonicalize() {
            Ok(m) => m,
            Err(_) => continue,
        };

        // If the IPFS repo path is within this mount, that's our disk.
        if repo_canon.starts_with(&mount_cmp) {
            let pct = (avail as f64 / total as f64) * 100.0;
            return (vec![(avail, total)], pct);
        }
    }

    (Vec::new(), 404.0)
}

pub fn disk_usage() -> (Vec<(u64, u64)>, f64) {
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

    fn set_ipfs_path<P: AsRef<Path>>(p: P) {
        unsafe {
            std::env::set_var("IPFS_PATH", p.as_ref());
        }
    }

    fn clear_ipfs_path() {
        unsafe {
            std::env::remove_var("IPFS_PATH");
        }
    }

    #[serial_test::serial]
    #[test]
    fn ipfs_path_not_in_any_disk_returns_404() {
        // Previously this used cwd; now we drive behavior via IPFS_PATH.
        let tmp = tempfile::tempdir().unwrap();

        // Point IPFS_PATH to a real dir that is NOT under any provided mount.
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        set_ipfs_path(&repo);

        let mut mock = MockDiskProvider {
            // unrelated mount (canonicalized for macOS safety)
            disks: vec![(
                100,
                200,
                std::path::PathBuf::from("/unrelated/mount")
                    .canonicalize()
                    .unwrap_or_else(|_| std::path::PathBuf::from("/unrelated/mount")),
            )],
        };

        let (disks, usage) = disk_usage_with(&mut mock);
        // With the new contract we return *only the IPFS disk*; since none matched, vec is empty.
        assert!(disks.is_empty());
        assert_eq!(usage, 404.0);

        clear_ipfs_path();
    }

    #[serial_test::serial]
    #[test]
    fn ipfs_path_in_disk_mount_returns_percentage() {
        // Now interpreted as: "IPFS_PATH in disk mount returns percentage".
        let tmp = tempfile::tempdir().unwrap();
        let mount_point = tmp.path().canonicalize().unwrap();

        // Put IPFS repo under this mount and point IPFS_PATH to it.
        let repo = mount_point.join("subdir").join("ipfs-repo");
        std::fs::create_dir_all(&repo).unwrap();
        set_ipfs_path(&repo);

        let mut mock = MockDiskProvider {
            disks: vec![(50, 100, mount_point.clone())],
        };

        // Debug info if it fails on CI
        tracing::info!("mount_point = {:?}", mount_point);
        tracing::info!("ipfs_repo   = {:?}", repo);

        let (disks, usage) = disk_usage_with(&mut mock);

        // New behavior: only the IPFS disk is returned
        assert_eq!(disks, vec![(50, 100)]);
        assert_eq!(
            usage, 50.0,
            "ipfs_repo ({:?}) did not match mount ({:?})",
            repo, mount_point
        );

        clear_ipfs_path();
    }

    #[serial_test::serial]
    #[test]
    fn returns_only_ipfs_disk_when_found() {
        let tmp = tempdir().unwrap();
        let mount_point = tmp.path().canonicalize().unwrap();

        // Put the IPFS repo under this mount
        let repo = mount_point.join("ipfs-repo");
        std::fs::create_dir_all(&repo).unwrap();
        set_ipfs_path(&repo);

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

        let (disks, usage) = disk_usage_with(&mut mock);
        assert_eq!(disks, vec![(50, 100)]);
        assert_eq!(usage, 50.0, "expected 50% available for the IPFS disk");

        clear_ipfs_path();
    }

    #[serial_test::serial]
    #[test]
    fn ipfs_path_not_in_any_disk_returns_empty_and_404() {
        let tmp = tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        set_ipfs_path(&repo);

        let mut mock = MockDiskProvider {
            // Only an unrelated mount exists
            disks: vec![(
                100,
                200,
                PathBuf::from("/unrelated/mount")
                    .canonicalize()
                    .unwrap_or_else(|_| PathBuf::from("/unrelated/mount")),
            )],
        };

        let (disks, usage) = disk_usage_with(&mut mock);
        assert!(disks.is_empty());
        assert_eq!(usage, 404.0);

        clear_ipfs_path();
    }

    #[serial_test::serial]
    #[test]
    fn does_not_panic_if_total_space_is_zero_returns_empty_and_404() {
        let tmp = tempdir().unwrap();
        let mount_point = tmp.path().canonicalize().unwrap();

        let repo = mount_point.join("repo-zero");
        std::fs::create_dir_all(&repo).unwrap();
        set_ipfs_path(&repo);

        let mut mock = MockDiskProvider {
            disks: vec![(0, 0, mount_point.clone())], // total = 0
        };

        let (disks, usage) = disk_usage_with(&mut mock);
        assert!(disks.is_empty());
        assert_eq!(usage, 404.0);

        clear_ipfs_path();
    }

    #[serial_test::serial]
    #[test]
    fn integration_sys_provider_returns_single_ipfs_disk_or_404() {
        // Point IPFS_PATH at a real temp dir so it exists
        let tmp = tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        set_ipfs_path(&repo);

        let (disks, usage) = disk_usage();

        if (usage - 404.0).abs() < f64::EPSILON {
            // Could not map the repo to a known mount on this platform/CI
            assert!(disks.is_empty());
        } else {
            assert_eq!(disks.len(), 1, "should only report the IPFS disk");
            let (avail, total) = disks[0];
            assert!(avail <= total);
            assert!((0.0..=100.0).contains(&usage));
        }

        clear_ipfs_path();
    }
}
