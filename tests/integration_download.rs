use std::process::Command;
use std::fs;
use chrono::NaiveDate;

fn bin() -> std::path::PathBuf {
    // Path to the built binary during `cargo test`
    assert_cmd::cargo::cargo_bin("polygon-data-downloader")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

fn env_with_key(cmd: &mut Command) -> &mut Command {
    // Expect POLYGON_API_KEY from environment. If missing, skip the test.
    let key = std::env::var("POLYGON_API_KEY").unwrap_or_else(|_| {
        // Use the built-in test skipping by returning early via panic with known pattern.
        // cargo does not have built-in skip, so we use `return` from calling site instead.
        // We'll never reach here; callers will check presence first.
        String::new()
    });
    cmd.env("POLYGON_API_KEY", key)
}

#[test]
fn unauthorized_asset_spx_should_fail() {
    if std::env::var("POLYGON_API_KEY").is_err() {
        eprintln!("skipped: set POLYGON_API_KEY to run integration tests");
        return;
    }
    // Attempt minute data for I:SPX which should be forbidden on basic/free plan
    let from = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
    let to = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
    let out = format!("output/I:SPX_{}_{}.csv", from, to);
    cleanup(&out);

    let mut cmd = Command::new(bin());
    env_with_key(&mut cmd);
    let assert = cmd
        .arg("download")
        .arg("-t").arg("I:SPX")
        .arg("-f").arg(from.to_string())
        .arg("-T").arg(to.to_string())
        .arg("--granularity").arg("minute")
        .arg("--format").arg("csv")
        .arg("-v")
        .output()
        .expect("failed to execute command");

    // Expect non-zero or stderr mentioning 403
    let success = assert.status.success();
    let stderr = String::from_utf8_lossy(&assert.stderr);
    let stdout = String::from_utf8_lossy(&assert.stdout);

    assert!(
        !success || stderr.contains("403") || stdout.contains("403"),
        "Expected failure/403 but got success. stdout=\n{}\nstderr=\n{}",
        stdout,
        stderr
    );

    // If it succeeded unexpectedly, ensure file is not created with data
    if fs::metadata(&out).is_ok() {
        let sz = fs::metadata(&out).unwrap().len();
        assert!(sz == 0 || !success, "Unauthorized download produced a non-empty file");
        let _ = fs::remove_file(&out);
    }
}

#[test]
fn minute_data_ndx_should_succeed() {
    if std::env::var("POLYGON_API_KEY").is_err() {
        eprintln!("skipped: set POLYGON_API_KEY to run integration tests");
        return;
    }
    // Use a single day in past market day to keep dataset small
    let from = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
    let to = from;
    let out = format!("output/I:NDX_{}_{}.csv", from, to);
    cleanup(&out);

    let mut cmd = Command::new(bin());
    env_with_key(&mut cmd);
    let output = cmd
        .arg("download")
        .arg("-t").arg("I:NDX")
        .arg("-f").arg(from.to_string())
        .arg("-T").arg(to.to_string())
        .arg("--granularity").arg("minute")
        .arg("--format").arg("csv")
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
    assert!(data.lines().count() > 1, "CSV should have header and at least one row");
    // Clean up large files
    let _ = fs::remove_file(&out);
}

#[test]
fn daily_data_aapl_should_succeed() {
    if std::env::var("POLYGON_API_KEY").is_err() {
        eprintln!("skipped: set POLYGON_API_KEY to run integration tests");
        return;
    }
    // AAPL daily data over a short range
    let from = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let to = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
    let out = format!("output/AAPL_{}_{}.csv", from, to);
    cleanup(&out);

    let mut cmd = Command::new(bin());
    env_with_key(&mut cmd);
    let output = cmd
        .arg("download")
        .arg("-t").arg("AAPL")
        .arg("-f").arg(from.to_string())
        .arg("-T").arg(to.to_string())
        .arg("--granularity").arg("day")
        .arg("--format").arg("csv")
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
    assert!(data.lines().count() > 1, "CSV should have header and at least one row");
    let _ = fs::remove_file(&out);
}


#[test]
fn missing_api_key_should_fail_fast() {
    // This test ensures behavior when POLYGON_API_KEY is not set.
    // It should fail early with an appropriate error.
    let from = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
    let to = from;

    let mut cmd = Command::new(bin());
    // Remove the API key from the child process explicitly, regardless of host env.
    cmd.env_remove("POLYGON_API_KEY");

    let output = cmd
        .arg("download")
        .arg("-t").arg("AAPL")
        .arg("-f").arg(from.to_string())
        .arg("-T").arg(to.to_string())
        .arg("--granularity").arg("day")
        .arg("--format").arg("csv")
        .output()
        .expect("failed to run child process");

    // Expect a non-success exit because API key is mandatory unless provided via --apikey
    assert!(
        !output.status.success(),
        "CLI unexpectedly succeeded without API key. stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // Also check for the specific error message surfaced by src/main.rs
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("API key not provided"),
        "Expected missing API key error. stderr=\n{}",
        err
    );
}


#[test]
fn csv_no_header_flag_should_omit_header() {
    if std::env::var("POLYGON_API_KEY").is_err() {
        eprintln!("skipped: set POLYGON_API_KEY to run integration tests");
        return;
    }
    let from = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let to = from;
    let out = format!("output/AAPL_{}_{}.csv", from, to);
    cleanup(&out);

    let mut cmd = Command::new(bin());
    env_with_key(&mut cmd);
    let output = cmd
        .arg("download")
        .arg("-t").arg("AAPL")
        .arg("-f").arg(from.to_string())
        .arg("-T").arg(to.to_string())
        .arg("--granularity").arg("day")
        .arg("--format").arg("csv")
        .arg("--no-header")
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
    let mut lines = data.lines();
    if let Some(first) = lines.next() {
        // The first line should not be the header when --no-header is used.
        assert!(!first.to_lowercase().contains("ticker,timestamp,open,high,low,close,volume"), "Header should be omitted with --no-header");
    } else {
        panic!("CSV file is empty");
    }
    let _ = fs::remove_file(&out);
}
