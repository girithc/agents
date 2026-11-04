//! `agentctl` — terminal client for the agent platform.
//!
//! Talks to the HTTP API (start it with `agentctl serve`). The web UI
//! will reuse the exact same endpoints.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use serde_json::{json, Value};
use std::io::Write;

#[derive(Parser)]
#[command(name = "agentctl", about = "Terminal client for the agent platform", version)]
struct Cli {
    /// Base URL of the agenthub API server.
    #[arg(long, env = "AGENTHUB_URL", default_value = "http://127.0.0.1:8080")]
    base_url: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the API server (same as `agenthub-api`).
    Serve {
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
    },
    #[command(subcommand)]
    Agent(AgentCmd),
    /// Run one input through an agent and stream the response.
    Run { agent: String, input: String },
    /// Interactive multi-turn terminal chat with an agent.
    Repl { agent: String },
    #[command(subcommand)]
    Runs(RunsCmd),
    #[command(subcommand)]
    Secret(SecretCmd),
    /// Rate a run (feedback drives the improve loop).
    Feedback {
        run_id: String,
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=5))]
        rating: i64,
        #[arg(long)]
        comment: Option<String>,
    },
    /// Improve an agent from accumulated feedback (the feedback loop).
    Improve {
        agent: String,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        prompt: String,
        #[arg(long, default_value = "meta")]
        provider: String,
        #[arg(long, default_value = "muse-spark-1.3-contributor")]
        model: String,
        #[arg(long, default_value_t = 0.7)]
        temperature: f64,
    },
    List,
    Get { id: String },
    Delete { id: String },
}

#[derive(Subcommand)]
enum RunsCmd {
    List {
        #[arg(long)]
        agent: Option<String>,
    },
    Show {
        id: String,
        /// Also print the persisted trace tree for the run.
        #[arg(long)]
        traces: bool,
    },
}

#[derive(Subcommand)]
enum SecretCmd {
    /// Store a secret value (reads from --value or stdin).
    Set {
        name: String,
        #[arg(long)]
        value: Option<String>,
    },
    List,
    Get {
        name: String,
        #[arg(long)]
        reveal: bool,
    },
    Delete {
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve { port, host } => {
            let data_dir = std::env::var("AGENTHUB_DATA_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from("./data"));
            agenthub_api::serve(agenthub_api::ServeConfig { host, port, data_dir }).await
        }
        Cmd::Agent(cmd) => agent_cmd(&cli.base_url, cmd).await,
        Cmd::Run { agent, input } => {
            let out = stream_run(&cli.base_url, &agent, &input, &[]).await?;
            println!();
            println!("{}", format_run_summary(&out));
            Ok(())
        }
        Cmd::Repl { agent } => repl(&cli.base_url, &agent).await,
        Cmd::Runs(cmd) => runs_cmd(&cli.base_url, cmd).await,
        Cmd::Secret(cmd) => secret_cmd(&cli.base_url, cmd).await,
        Cmd::Feedback { run_id, rating, comment } => {
            let v = post_json(
                &cli.base_url,
                &format!("/v1/runs/{run_id}/feedback"),
                &json!({ "rating": rating, "comment": comment }),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            Ok(())
        }
        Cmd::Improve { agent, dry_run } => {
            let v = post_json(
                &cli.base_url,
                &format!("/v1/agents/{agent}/improve"),
                &json!({ "dry_run": dry_run }),
            )
            .await?;
            println!("improve run:   {}", v["run_id"].as_str().unwrap_or("?"));
            println!("feedback count: {}", v["feedback_count"]);
            println!("applied:       {}", v["applied"]);
            println!("--- proposed system prompt ---");
            println!("{}", v["proposed_system_prompt"].as_str().unwrap_or(""));
            Ok(())
        }
    }
}

// ---------------------------------------------------------------- commands

async fn agent_cmd(base: &str, cmd: AgentCmd) -> Result<()> {
    match cmd {
        AgentCmd::Create { name, prompt, provider, model, temperature } => {
            let v = post_json(
                base,
                "/v1/agents",
                &json!({ "name": name, "system_prompt": prompt, "provider": provider, "model": model, "temperature": temperature }),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        AgentCmd::List => {
            let v = api_get(base, "/v1/agents").await?;
            for a in v["agents"].as_array().unwrap_or(&vec![]) {
                println!(
                    "{}\t{}\tprovider={} model={} v={}",
                    a["id"].as_str().unwrap_or("?"),
                    a["name"].as_str().unwrap_or("?"),
                    a["provider"].as_str().unwrap_or("?"),
                    a["model"].as_str().unwrap_or("?"),
                    a["version"],
                );
            }
        }
        AgentCmd::Get { id } => {
            let v = api_get(base, &format!("/v1/agents/{id}")).await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        AgentCmd::Delete { id } => {
            let _ = api_delete(base, &format!("/v1/agents/{id}")).await?;
            println!("deleted {id}");
        }
    }
    Ok(())
}

async fn runs_cmd(base: &str, cmd: RunsCmd) -> Result<()> {
    match cmd {
        RunsCmd::List { agent } => {
            let qs = agent
                .map(|a| format!("?agent_id={a}"))
                .unwrap_or_default();
            let v = api_get(base, &format!("/v1/runs{qs}")).await?;
            for r in v["runs"].as_array().unwrap_or(&vec![]) {
                println!(
                    "{}\t{}\tkind={}\tagent={} started={}",
                    r["id"].as_str().unwrap_or("?"),
                    r["status"].as_str().unwrap_or("?"),
                    r["kind"].as_str().unwrap_or("regular"),
                    r["agent_id"].as_str().unwrap_or("?"),
                    r["started_at"],
                );
            }
        }
        RunsCmd::Show { id, traces } => {
            let mut v = api_get(base, &format!("/v1/runs/{id}")).await?;
            // Response is the run fields + "messages"; print run fields only.
            if let Some(obj) = v.as_object_mut() {
                obj.remove("messages");
            }
            println!("{}", serde_json::to_string_pretty(&v)?);
            println!("--- messages ---");
            for m in v["messages"].as_array().unwrap_or(&vec![]) {
                let role = m["role"].as_str().unwrap_or("?");
                let marker = if role == "reasoning" { " (thinking)" } else { "" };
                println!(
                    "[{}{}] {}",
                    role,
                    marker,
                    m["content"].as_str().unwrap_or("")
                );
            }
            if traces {
                let t = api_get(base, &format!("/v1/runs/{id}/traces")).await?;
                print_trace_tree(&t);
            }
        }
    }
    Ok(())
}

async fn secret_cmd(base: &str, cmd: SecretCmd) -> Result<()> {
    match cmd {
        SecretCmd::Set { name, value } => {
            let value = match value {
                Some(v) => v,
                None => {
                    print!("value for '{name}': ");
                    std::io::stdout().flush()?;
                    let mut line = String::new();
                    std::io::stdin().read_line(&mut line)?;
                    line.trim().to_string()
                }
            };
            let _ = put_json(base, &format!("/v1/secrets/{name}"), &json!({ "value": value })).await?;
            println!("stored '{name}' (encrypted at rest)");
        }
        SecretCmd::List => {
            let v = api_get(base, "/v1/secrets").await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        SecretCmd::Get { name, reveal } => {
            let v = api_get(
                base,
                &format!("/v1/secrets/{name}?reveal={reveal}"),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        SecretCmd::Delete { name } => {
            let _ = api_delete(base, &format!("/v1/secrets/{name}")).await?;
            println!("deleted {name}");
        }
    }
    Ok(())
}

async fn repl(base: &str, agent: &str) -> Result<()> {
    println!("repl with '{agent}' — type /quit to exit, /reset to clear history");
    let mut history: Vec<Value> = Vec::new();
    loop {
        print!("\x1b[2m❯\x1b[0m ");
        std::io::stdout().flush()?;
        let mut line = String::new();
        if std::io::stdin().read_line(&mut line)? == 0 {
            break;
        }
        let input = line.trim();
        match input {
            "" => continue,
            "/quit" | "/exit" | "/q" => break,
            "/reset" => {
                history.clear();
                println!("(history cleared)");
                continue;
            }
            input => {
                println!();
                match stream_run(base, agent, input, &history).await {
                    Ok(out) => {
                        println!();
                        history.push(json!({ "role": "user", "content": input }));
                        history.push(json!({ "role": "assistant", "content": out }));
                    }
                    Err(e) => eprintln!("\x1b[31merror:\x1b[0m {e:#}"),
                }
                println!();
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- http

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn check(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        let body = resp.text().await.unwrap_or_default();
        let msg = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|v| v["error"].as_str().map(|s| s.to_string()))
            .unwrap_or(body);
        bail!("http {status}: {msg}");
    }
}

async fn api_get(base: &str, path: &str) -> Result<Value> {
    let resp = client().get(format!("{base}{path}")).send().await?;
    Ok(resp.json().await.context("decoding JSON response")?)
}

async fn api_delete(base: &str, path: &str) -> Result<Value> {
    let resp = client().delete(format!("{base}{path}")).send().await?;
    check(resp).await?;
    Ok(json!({}))
}

async fn post_json(base: &str, path: &str, body: &Value) -> Result<Value> {
    let resp = client().post(format!("{base}{path}")).json(body).send().await?;
    check(resp).await?.json().await.context("decoding JSON response")
}

async fn put_json(base: &str, path: &str, body: &Value) -> Result<Value> {
    let resp = client().put(format!("{base}{path}")).json(body).send().await?;
    check(resp).await?.json().await.context("decoding JSON response")
}

/// POST to the streaming run endpoint, print deltas live, return the final
/// output text.
async fn stream_run(base: &str, agent: &str, input: &str, history: &[Value]) -> Result<String> {
    let resp = client()
        .post(format!("{base}/v1/agents/{agent}/runs/stream"))
        .header("Accept", "text/event-stream")
        .json(&json!({ "input": input, "history": history }))
        .send()
        .await?;
    let resp = check(resp).await?;

    let mut out = String::new();
    let mut run_id = String::new();
    let mut buf = String::new();
    let mut event_name = String::new();
    let mut data = String::new();
    let mut stream = resp.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = buf.find('\n') {
            let line = buf[..idx].trim_end_matches('\r').to_string();
            buf.drain(..=idx);
            if line.is_empty() {
                if !data.is_empty() {
                    let payload: Value = match serde_json::from_str(&data) {
                        Ok(v) => v,
                        Err(_) => {
                            event_name.clear();
                            data.clear();
                            continue;
                        }
                    };
                    match payload["type"].as_str().unwrap_or(&event_name) {
                        "run" => {
                            run_id = payload["run_id"].as_str().unwrap_or("").to_string();
                            eprintln!("\x1b[2mrun {run_id}\x1b[0m");
                        }
                        "delta" => {
                            let text = payload["text"].as_str().unwrap_or("");
                            out.push_str(text);
                            print!("{text}");
                            std::io::stdout().flush()?;
                        }
                        "reasoning" => {
                            let text = payload["text"].as_str().unwrap_or("");
                            print!("\x1b[2m\x1b[3m{text}\x1b[0m");
                            std::io::stdout().flush()?;
                        }
                        "done" => {
                            return Ok(out);
                        }
                        "error" => {
                            bail!(
                                "{}",
                                payload["error"].as_str().unwrap_or("unknown stream error")
                            );
                        }
                        _ => {}
                    }
                }
                event_name.clear();
                data.clear();
            } else if let Some(rest) = line.strip_prefix("event:") {
                event_name = rest.trim().to_string();
            } else if let Some(rest) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(rest.trim_start());
            }
        }
    }

    if run_id.is_empty() && out.is_empty() {
        bail!("stream ended without a done event");
    }
    Ok(out)
}

fn format_run_summary(output: &str) -> String {
    format!("\x1b[2m--- {}\x1b[0m", output.len().to_string() + " chars")
}

// ---------------------------------------------------------------- traces

fn print_trace_tree(traces: &Value) {
    println!("--- traces ---");
    let spans = traces["spans"].as_array().cloned().unwrap_or_default();
    let events = traces["events"].as_array().cloned().unwrap_or_default();

    // depth per span via parent chain
    let mut depths: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    fn depth_of(
        span: &Value,
        spans: &[Value],
        depths: &mut std::collections::HashMap<String, usize>,
    ) -> usize {
        let id = span["span_id"].as_str().unwrap_or("").to_string();
        if let Some(d) = depths.get(&id) {
            return *d;
        }
        let d = match span["parent_id"].as_str() {
            Some(pid) => spans
                .iter()
                .find(|s| s["span_id"].as_str() == Some(pid))
                .map(|p| depth_of(p, spans, depths) + 1)
                .unwrap_or(0),
            None => 0,
        };
        depths.insert(id, d);
        d
    }

    for span in &spans {
        let d = depth_of(span, &spans, &mut depths);
        let dur = match (span["start_ts"].as_i64(), span["end_ts"].as_i64()) {
            (Some(s), Some(e)) => format!("{}ms", e - s),
            _ => "…".into(),
        };
        println!(
            "{}{} \x1b[2m[{}]\x1b[0m {}",
            "  ".repeat(d),
            span["name"].as_str().unwrap_or("?"),
            dur,
            span["fields"]
        );
        for ev in &events {
            if ev["span_id"].as_str() == span["span_id"].as_str() {
                println!(
                    "{}• {} \x1b[2m{}{}\x1b[0m",
                    "  ".repeat(d + 1),
                    ev["message"].as_str().unwrap_or(""),
                    if ev["fields"].as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                        format!(" {}", ev["fields"])
                    } else {
                        String::new()
                    },
                    ""
                );
            }
        }
    }
}
