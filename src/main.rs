use std::{env, thread, time::Duration};

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use reqwest::Url;
use serde::Deserialize;

/// Polygon.io minute data downloader
///
/// Examples:
///   polygon-data-downloader download --ticker I:SPX --from 2024-01-01 --to 2024-01-03 --out spx.csv
///   POLYGON_API_KEY=... polygon-data-downloader download -t I:NDX -f 2024-02-01 -T 2024-02-01 --json
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

    /// Respect free plan by waiting between requests (~12s for 5 req/min)
    #[arg(long = "rate-limit-wait-secs", default_value_t = 12u64)]
    wait_secs: u64,

    /// Verbose logging
    #[arg(short = 'v', long = "verbose", action = ArgAction::Count)]
    verbose: u8,
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
    v: f64,   // volume
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

async fn download(args: DownloadArgs) -> Result<()> {
    let api_key = args
        .api_key
        .or_else(|| env::var("POLYGON_API_KEY").ok())
        .ok_or_else(|| anyhow!("API key not provided. Use --apikey or set POLYGON_API_KEY."))?;

    let out_path = args.out.unwrap_or_else(|| {
        let ext = match args.format { OutputFormat::Csv => "csv", OutputFormat::Json => "json" };
        format!("{}_{}_{}.{}", args.ticker, args.from, args.to, ext)
    });

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

        if !wrote_any && !results.is_empty() {
            // Open sink lazily
            match args.format {
                OutputFormat::Csv => {
                    let file = std::fs::File::create(&out_path)
                        .with_context(|| format!("Cannot create {}", out_path))?;
                    writer_csv = csv::Writer::from_writer(file);
                    // write header
                    writer_csv.write_record(["timestamp", "open", "high", "low", "close", "volume"]).ok();
                    sink = Sink::Csv(writer_csv);
                }
                OutputFormat::Json => {
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
                for r in &results {
                    w.write_record(&[
                        r.t.to_string(),
                        r.o.to_string(),
                        r.h.to_string(),
                        r.l.to_string(),
                        r.c.to_string(),
                        r.v.to_string(),
                    ])
                    .ok();
                }
                w.flush().ok();
            }
            Sink::Json(f) => {
                use std::io::Write;
                for (i, r) in results.iter().enumerate() {
                    if wrote_any || i > 0 { write!(f, ",").ok(); }
                    let obj = serde_json::json!({
                        "timestamp": r.t,
                        "open": r.o,
                        "high": r.h,
                        "low": r.l,
                        "close": r.c,
                        "volume": r.v,
                        "vw": r.vw,
                        "n": r.n,
                    });
                    write!(f, "{}", obj).ok();
                }
                f.flush().ok();
            }
            Sink::None => {}
        }
        wrote_any = wrote_any || !results.is_empty();

        // Determine next page
        next = match aggs.next_url {
            Some(next_url) => Some(Url::parse(&next_url)?),
            None => None,
        };

        if next.is_some() {
            if args.verbose > 0 { eprintln!("Sleeping {}s to respect rate limit...", args.wait_secs); }
            thread::sleep(Duration::from_secs(args.wait_secs));
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
    } else {
        eprintln!("Saved to {}", out_path);
    }

    Ok(())
}
