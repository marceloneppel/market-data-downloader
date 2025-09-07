use std::{env, time::Duration};

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc, TimeZone, Datelike};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use reqwest::Url;
use serde::Deserialize;

/// Polygon.io minute data downloader
///
/// Examples:
///   polygon-data-downloader download --apikey=... --ticker AAPL --from 2024-01-01 --to 2024-01-03 --out aapl.csv
///   POLYGON_API_KEY=... polygon-data-downloader download -t I:NDX -f 2024-02-01 -T 2024-02-01 --format json
#[derive(Parser, Debug)]
#[command(name = "polygon-data-downloader", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Download minute-level aggregates for an index
    Download(DownloadArgs),
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum OutputFormat { Csv, Json }

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Granularity { Minute, Day }

#[derive(Parser, Debug)]
struct DownloadArgs {
    /// Polygon index ticker, e.g. I:SPX, I:NDX, I:VIX
    #[arg(short = 't', long = "ticker")]
    ticker: String,

    /// Start date (YYYY-MM-DD)
    #[arg(short = 'f', long = "from")]
    from: NaiveDate,

    /// End date inclusive (YYYY-MM-DD)
    #[arg(short = 'T', long = "to")]
    to: NaiveDate,

    /// Polygon API key (can use env POLYGON_API_KEY)
    #[arg(short = 'k', long = "apikey")]
    api_key: Option<String>,

    /// Output file path (defaults to ticker_from_to.csv or .json)
    #[arg(short = 'o', long = "out")]
    out: Option<String>,

    /// Output format
    #[arg(long = "format", value_enum, default_value_t = OutputFormat::Csv)]
    format: OutputFormat,

    /// Data granularity (minute or day)
    #[arg(long = "granularity", value_enum, default_value_t = Granularity::Minute)]
    granularity: Granularity,

    /// Omit header row in CSV output
    #[arg(long = "no-header", default_value_t = false)]
    no_header: bool,

    /// Respect free plan by waiting between requests (~12s for 5 req/min)
    #[arg(long = "rate-limit-wait-secs", default_value_t = 12u64)]
    wait_secs: u64,

    /// Verbose logging
    #[arg(short = 'v', long = "verbose", action = ArgAction::Count)]
    verbose: u8,

    /// Maximum number of decimal places for OHLCV values
    #[arg(long = "max-decimals", default_value_t = 2u8)]
    max_decimals: u8,

    /// Split output into per-day CSV files under output/YYYY/MM/TICKER_YYYY-MM-DD.csv
    #[arg(long = "split-by-day", default_value_t = false)]
    split_by_day: bool,
}

#[derive(Debug, Deserialize)]
struct AggsResponse {
    results: Option<Vec<Agg>>,
    next_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Agg {
    t: i64,   // timestamp in ms
    o: f64,   // open
    h: f64,   // high
    l: f64,   // low
    c: f64,   // close
    v: Option<f64>,   // volume may be missing for indices
    vw: Option<f64>,
    n: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Download(args) => download(args).await,
    }
}

// Format milliseconds since epoch into UTC timestamp string
pub(crate) fn fmt_ts(ms: i64) -> String {
    if let Some(dt) = Utc.timestamp_millis_opt(ms).single() {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        ms.to_string()
    }
}

pub(crate) fn compute_out_path(ticker: &str, from: NaiveDate, to: NaiveDate, format: OutputFormat, out: &Option<String>) -> String {
    match out {
        Some(p) => p.clone(),
        None => {
            let ext = match format { OutputFormat::Csv => "csv", OutputFormat::Json => "json" };
            // Place files under output/ instead of project root
            format!("output/{}_{}_{}.{}", ticker, from, to, ext)
        }
    }
}

pub(crate) fn ensure_api_key_present(url: &mut Url, api_key: &str) {
    let has_key = url.query_pairs().any(|(k, _)| k == "apiKey");
    if !has_key {
        url.query_pairs_mut().append_pair("apiKey", api_key);
    }
}

async fn download(args: DownloadArgs) -> Result<()> {
    let api_key = args
        .api_key
        .or_else(|| env::var("POLYGON_API_KEY").ok())
        .ok_or_else(|| anyhow!("API key not provided. Use --apikey or set POLYGON_API_KEY."))?;

    if args.split_by_day && matches!(args.format, OutputFormat::Json) {
        return Err(anyhow!("--split-by-day currently supports CSV format only"));
    }

    let out_path = compute_out_path(&args.ticker, args.from, args.to, args.format, &args.out);

    let client = reqwest::Client::builder()
        .user_agent("polygon-data-downloader/0.1")
        .build()?;

    let mut writer_csv;
    let mut wrote_any = false;

    enum Sink { Csv(csv::Writer<std::fs::File>), Json(std::fs::File), None }
    let mut sink: Sink = Sink::None;

    // Construct initial URL for v2 aggs range with selected granularity
    let gran = match args.granularity { Granularity::Minute => "minute", Granularity::Day => "day" };
    let mut url = Url::parse(&format!(
        "https://api.polygon.io/v2/aggs/ticker/{}/range/1/{}/{}/{}",
        urlencoding::encode(&args.ticker), gran, args.from, args.to
    ))?;
    url.query_pairs_mut()
        .append_pair("adjusted", "true")
        .append_pair("sort", "asc")
        .append_pair("limit", "50000")
        .append_pair("apiKey", &api_key);

    let mut page = 0usize;
    let mut next: Option<Url> = Some(url);

    loop {
        let Some(fetch_url) = next.take() else { break };
        page += 1;
        if args.verbose > 0 { eprintln!("Fetching page {}: {}", page, fetch_url); }
        let resp = client
            .get(fetch_url.clone())
            .send()
            .await
            .with_context(|| format!("Request failed: {}", fetch_url))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status.as_u16() == 403 {
                return Err(anyhow!(
                    "HTTP 403 Forbidden: {}\nHint: Your API key may not be entitled to this data. Try:\n- Using --granularity day (daily aggregates) instead of minute\n- Using a different ticker (e.g., equities like AAPL)\n- Upgrading your Polygon plan for minute/index data\nRequest URL: {}",
                    text, fetch_url
                ));
            }
            return Err(anyhow!("HTTP {}: {}", status, text));
        }

        let aggs: AggsResponse = resp.json().await.with_context(|| "Invalid JSON from API")?;
        let results = aggs.results.unwrap_or_default();

        if args.split_by_day {
            // Write each record into per-day CSV under output/YYYY/MM/TICKER_YYYY-MM-DD.csv
            use std::fs::{OpenOptions, create_dir_all, metadata};
            use std::io::Write as _;
            let prec = args.max_decimals as usize;
            for r in &results {
                if let Some(dt) = Utc.timestamp_millis_opt(r.t).single() {
                    let date = dt.date_naive();
                    let year = date.year();
                    let month = date.month();
                    let day = date.day();
                    let dir = format!("output/{}/{:02}", year, month);
                    create_dir_all(&dir).with_context(|| format!("Cannot create directory {}", dir))?;
                    let file_path = format!("{}/{}_{}-{:02}-{:02}.csv", dir, args.ticker, year, month, day);
                    let mut file = OpenOptions::new().create(true).append(true).open(&file_path)
                        .with_context(|| format!("Cannot open {}", file_path))?;
                    let is_new = metadata(&file_path).map(|m| m.len() == 0).unwrap_or(true);
                    // Write header if new and not omitted
                    if is_new && !args.no_header {
                        writeln!(file, "ticker,timestamp,open,high,low,close,volume").ok();
                    }
                    let ts = fmt_ts(r.t);
                    let o = format!("{:.1$}", r.o, prec);
                    let h = format!("{:.1$}", r.h, prec);
                    let l = format!("{:.1$}", r.l, prec);
                    let c = format!("{:.1$}", r.c, prec);
                    let v = match r.v { Some(val) => format!("{:.1$}", val, prec), None => String::from("") };
                    writeln!(file, "{},{},{},{},{},{},{}", args.ticker, ts, o, h, l, c, v).ok();
                }
            }
            wrote_any = wrote_any || !results.is_empty();
        } else {
            if !wrote_any && !results.is_empty() {
                // Open sink lazily
                match args.format {
                    OutputFormat::Csv => {
                        // Ensure parent directory exists if path includes directories
                        if let Some(parent) = std::path::Path::new(&out_path).parent() {
                            if !parent.as_os_str().is_empty() {
                                std::fs::create_dir_all(parent)
                                    .with_context(|| format!("Cannot create directory {}", parent.display()))?;
                            }
                        }
                        let file = std::fs::File::create(&out_path)
                            .with_context(|| format!("Cannot create {}", out_path))?;
                        writer_csv = csv::Writer::from_writer(file);
                        // write header (unless omitted)
                        if !args.no_header {
                            writer_csv.write_record(["ticker", "timestamp", "open", "high", "low", "close", "volume"]).ok();
                        }
                        sink = Sink::Csv(writer_csv);
                    }
                    OutputFormat::Json => {
                        // Ensure parent directory exists if path includes directories
                        if let Some(parent) = std::path::Path::new(&out_path).parent() {
                            if !parent.as_os_str().is_empty() {
                                std::fs::create_dir_all(parent)
                                    .with_context(|| format!("Cannot create directory {}", parent.display()))?;
                            }
                        }
                        let file = std::fs::File::create(&out_path)
                            .with_context(|| format!("Cannot create {}", out_path))?;
                        // Write opening bracket for an array
                        use std::io::Write;
                        write!(&file, "[").ok();
                        sink = Sink::Json(file);
                    }
                }
            }

            match &mut sink {
                Sink::Csv(w) => {
                    let prec = args.max_decimals as usize;
                    for r in &results {
                        let ts = fmt_ts(r.t);
                        let o = format!("{:.1$}", r.o, prec);
                        let h = format!("{:.1$}", r.h, prec);
                        let l = format!("{:.1$}", r.l, prec);
                        let c = format!("{:.1$}", r.c, prec);
                        let v = match r.v {
                            Some(val) => format!("{:.1$}", val, prec),
                            None => String::new(),
                        };
                        w.write_record(&[
                            args.ticker.as_str(),
                            ts.as_str(),
                            o.as_str(),
                            h.as_str(),
                            l.as_str(),
                            c.as_str(),
                            v.as_str(),
                        ])
                        .ok();
                    }
                    w.flush().ok();
                }
                Sink::Json(f) => {
                    use std::io::Write;
                    let prec = args.max_decimals as i32;
                    let pow = 10f64.powi(prec);
                    let round_to = |x: f64| (x * pow).round() / pow;
                    for (i, r) in results.iter().enumerate() {
                        if wrote_any || i > 0 { write!(f, ",").ok(); }
                        let obj = serde_json::json!({
                            "timestamp": fmt_ts(r.t),
                            "open": round_to(r.o),
                            "high": round_to(r.h),
                            "low": round_to(r.l),
                            "close": round_to(r.c),
                            "volume": r.v.map(|v| round_to(v)),
                            "vw": r.vw.map(|x| round_to(x)),
                            "n": r.n,
                        });
                        write!(f, "{}", obj).ok();
                    }
                    f.flush().ok();
                }
                Sink::None => {}
            }
            wrote_any = wrote_any || !results.is_empty();
        }

        // Determine next page
        next = match aggs.next_url {
            Some(next_url) => {
                let mut u = Url::parse(&next_url)?;
                // Ensure the API key is always included on subsequent pages
                ensure_api_key_present(&mut u, &api_key);
                Some(u)
            }
            None => None,
        };

        if next.is_some() {
            if args.verbose > 0 { eprintln!("Sleeping {}s to respect rate limit...", args.wait_secs); }
            tokio::time::sleep(Duration::from_secs(args.wait_secs)).await;
        } else {
            if args.verbose > 0 { eprintln!("Done. Total pages: {}", page); }
            break;
        }
    }

    // Close JSON array if needed
    if let Sink::Json(mut f) = sink {
        use std::io::Write;
        write!(&mut f, "]").ok();
        f.flush().ok();
    }

    if !wrote_any {
        eprintln!("No data returned for {} between {} and {}", args.ticker, args.from, args.to);
    } else if args.split_by_day {
        eprintln!("Saved per-day CSV files under output/YYYY/MM");
    } else {
        eprintln!("Saved to {}", out_path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_fmt_ts_zero() {
        assert_eq!(fmt_ts(0), "1970-01-01 00:00:00");
    }

    #[test]
    fn test_fmt_ts_known() {
        // 2024-04-01 00:00:00 UTC in ms
        let ts = 1711929600000i64;
        assert_eq!(fmt_ts(ts), "2024-04-01 00:00:00");
    }

    #[test]
    fn test_compute_out_path_defaults_csv() {
        let d1 = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        let out = compute_out_path("I:NDX", d1, d2, OutputFormat::Csv, &None);
        assert_eq!(out, "output/I:NDX_2025-01-01_2025-01-31.csv");
    }

    #[test]
    fn test_compute_out_path_defaults_json() {
        let d1 = NaiveDate::from_ymd_opt(2024, 2, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 2).unwrap();
        let out = compute_out_path("AAPL", d1, d2, OutputFormat::Json, &None);
        assert_eq!(out, "output/AAPL_2024-02-01_2024-02-02.json");
    }

    #[test]
    fn test_compute_out_path_respects_explicit() {
        let d1 = NaiveDate::from_ymd_opt(2025, 9, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2025, 9, 4).unwrap();
        let explicit = Some(String::from("custom.csv"));
        let out = compute_out_path("I:SPX", d1, d2, OutputFormat::Csv, &explicit);
        assert_eq!(out, "custom.csv");
    }

    #[test]
    fn test_ensure_api_key_present_adds_when_missing() {
        let mut u = Url::parse("https://example.com/path?foo=1").unwrap();
        ensure_api_key_present(&mut u, "KEY123");
        let query: Vec<_> = u.query_pairs().collect();
        assert!(query.iter().any(|(k,v)| k=="apiKey" && v=="KEY123"));
    }

    #[test]
    fn test_ensure_api_key_present_keeps_when_present() {
        let mut u = Url::parse("https://example.com/path?apiKey=ABC&x=1").unwrap();
        ensure_api_key_present(&mut u, "SHOULD_NOT_OVERRIDE");
        // ensure existing value is not overridden
        let pairs: Vec<_> = u.query_pairs().collect();
        let found: Vec<_> = pairs.into_iter().filter(|(k,_)| k=="apiKey").collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1, "ABC");
    }

    #[test]
    fn test_cli_parses_no_header_flag() {
        // default is false
        let cli = Cli::parse_from([
            "polygon-data-downloader",
            "download",
            "-t","AAPL",
            "-f","2025-01-01",
            "-T","2025-01-01",
        ]);
        let Commands::Download(args) = cli.command;
        assert!(!args.no_header);

        // explicit true via --no-header
        let cli2 = Cli::parse_from([
            "polygon-data-downloader",
            "download",
            "-t","AAPL",
            "-f","2025-01-01",
            "-T","2025-01-01",
            "--no-header",
        ]);
        let Commands::Download(args2) = cli2.command;
        assert!(args2.no_header);
    }

    #[test]
    fn test_cli_default_max_decimals() {
        let cli = Cli::parse_from([
            "polygon-data-downloader",
            "download",
            "-t","AAPL",
            "-f","2025-01-01",
            "-T","2025-01-01",
        ]);
        let Commands::Download(args) = cli.command;
        assert_eq!(args.max_decimals, 2);
    }

    #[test]
    fn test_cli_override_max_decimals() {
        let cli = Cli::parse_from([
            "polygon-data-downloader",
            "download",
            "-t","AAPL",
            "-f","2025-01-01",
            "-T","2025-01-01",
            "--max-decimals","4",
        ]);
        let Commands::Download(args) = cli.command;
        assert_eq!(args.max_decimals, 4);
    }

    #[test]
    fn test_cli_split_by_day_flag() {
        // default is false
        let cli = Cli::parse_from([
            "polygon-data-downloader",
            "download",
            "-t","AAPL",
            "-f","2025-01-01",
            "-T","2025-01-01",
        ]);
        let Commands::Download(args) = cli.command;
        assert!(!args.split_by_day);

        // explicit
        let cli2 = Cli::parse_from([
            "polygon-data-downloader",
            "download",
            "-t","AAPL",
            "-f","2025-01-01",
            "-T","2025-01-01",
            "--split-by-day",
        ]);
        let Commands::Download(args2) = cli2.command;
        assert!(args2.split_by_day);
    }
}
