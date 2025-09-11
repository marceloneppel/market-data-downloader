use chrono::NaiveDate;
use std::fs;
use std::process::Command;

fn bin() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("market-data-downloader")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

fn env_with_td_key(cmd: &mut Command) -> &mut Command {
    let key = std::env::var("TWELVEDATA_API_KEY").unwrap_or_else(|_| String::new());
    cmd.env("TWELVEDATA_API_KEY", key)
}

#[test]
fn td_missing_api_key_should_fail_fast() {
    // Ensure it fails without TWELVEDATA_API_KEY when provider=twelvedata
    let from = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let to = from;

    let mut cmd = Command::new(bin());
    cmd.env_remove("TWELVEDATA_API_KEY");

    let output = cmd
        .arg("download")
        .arg("-t")
        .arg("AAPL")
        .arg("-f")
        .arg(from.to_string())
        .arg("-T")
        .arg(to.to_string())
        .arg("--granularity")
        .arg("day")
        .arg("--format")
        .arg("csv")
        .arg("--provider")
        .arg("twelvedata")
        .output()
        .expect("failed to run child process");

    assert!(
        !output.status.success(),
        "CLI unexpectedly succeeded without Twelve Data API key. stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("TWELVEDATA_API_KEY"),
        "Expected missing TWELVEDATA_API_KEY error. stderr=\n{}",
        err
    );
}

#[test]
fn td_daily_data_aapl_should_succeed_small_range() {
    if std::env::var("TWELVEDATA_API_KEY").is_err() {
        eprintln!("skipped: set TWELVEDATA_API_KEY to run Twelve Data integration tests");
        return;
    }

    let from = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let to = from;
    let out = format!("output/AAPL_{}_{}.csv", from, to);
    cleanup(&out);

    let mut cmd = Command::new(bin());
    env_with_td_key(&mut cmd);
    let output = cmd
        .arg("download")
        .arg("-t")
        .arg("AAPL")
        .arg("-f")
        .arg(from.to_string())
        .arg("-T")
        .arg(to.to_string())
        .arg("--granularity")
        .arg("day")
        .arg("--format")
        .arg("csv")
        .arg("--provider")
        .arg("twelvedata")
        .output()
        .expect("failed to run");

    if !output.status.success() {
        panic!(
            "Download failed. stdout=\n{}\nstderr=\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let data = fs::read_to_string(&out).expect("output file missing");
    assert!(
        data.lines().count() > 1,
        "CSV should have header and at least one row"
    );
    let _ = fs::remove_file(&out);
}
