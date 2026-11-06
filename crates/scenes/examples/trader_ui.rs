//! Live two-column terminal UI for the trader world.
//!
//! Left column: the message feed — every word exchanged, chronological.
//! Right column: the board — item status, live bid (buyer's last offer)
//! and ask (seller's last ask), the deal; plus each agent's live state.
//!
//! Plain ANSI redraw, no extra deps. Runs until the deal closes (+5s) or
//! TRADER_UI_SECS elapses (default 120).

use agenthub_runtime::{LlmBrain, Scene, Supervisor};
use agenthub_providers::meta::MetaProvider;
use agenthub_providers::registry::Registry;
use agenthub_scenes::trader::TraderScene;
use agenthub_storage::Store;
use std::sync::Arc;
use std::time::Duration;

const W_LEFT: usize = 62;
const W_RIGHT: usize = 46;

/// The last $-amount mentioned in a sentence ("I could do $85") —
/// prices ride the words, so read them from the words.
fn last_dollar(s: &str) -> Option<f64> {
    let lower = s.replace(',', "");
    let mut found = None;
    let chars: Vec<char> = lower.chars().collect();
    for i in 0..chars.len() {
        if chars[i] == '$' {
            let num: String = chars[i + 1..]
                .iter()
                .take_while(|c| c.is_ascii_digit() || **c == '.')
                .collect();
            if let Ok(v) = num.parse::<f64>() {
                found = Some(v);
            }
        }
    }
    found
}

/// Formal offers from the ledger (as (party, price)) — fallback source.
async fn feed_offers(
    names: &std::collections::HashMap<String, String>,
    s: &Store,
    env_id: &str,
) -> Vec<(String, String)> {
    s.list_changes(env_id, Some("offer_made"), 10)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|c| {
            let p: serde_json::Value = serde_json::from_str(&c.payload).ok()?;
            let who = p.get("from").and_then(|v| v.as_str()).and_then(|f| names.get(f).cloned())?;
            let price = p.get("price").and_then(|v| v.as_f64())?;
            Some((who, format!("I formally offer ${price:.0}")))
        })
        .collect()
}

fn pad(s: &str, w: usize) -> String {
    let t: String = s.chars().take(w).collect();
    format!("{t:<w$}", w = w)
}

fn clip(s: &str, w: usize) -> String {
    let t: String = s.chars().take(w).collect();
    if s.chars().count() > w {
        format!("{}…", &t[..t.chars().count().saturating_sub(1)])
    } else {
        t
    }
}

/// Wrap text to width, returning lines.
fn wrap(s: &str, w: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for para in s.split('\n') {
        let mut cur = String::new();
        for word in para.split_whitespace() {
            let cand_len = cur.chars().count() + word.chars().count() + usize::from(!cur.is_empty());
            if cand_len > w && !cur.is_empty() {
                lines.push(cur.clone());
                cur.clear();
            }
            if !cur.is_empty() {
                cur.push(' ');
            }
            cur.push_str(word);
        }
        lines.push(cur);
    }
    lines
}

#[tokio::main]
async fn main() {
    let run_secs: u64 = std::env::var("TRADER_UI_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120);

    let dir = std::env::temp_dir().join(format!("agenthub-ui-{}", uuid::Uuid::new_v4()));
    let s: Arc<Store> = Arc::new(
        Store::open(&dir.join("test.db"), agenthub_storage::Vault::open(&dir.join("master.key")).unwrap())
            .expect("open store"),
    );
    s.put_secret("meta.api_key", &agenthub_runtime::model_api_key().unwrap_or_default())
        .await
        .unwrap();

    let scene = TraderScene;
    let world = scene.setup(&s).await.unwrap();

    let sup = Supervisor::new(
        s.clone(),
        world.env.clone(),
        Arc::new({
            let s2 = s.clone();
            let domain = scene.domain_tools();
            move |_a| {
                let registry = Registry::new();
                registry.register(Arc::new(MetaProvider::new(None)));
                Arc::new(LlmBrain::new(s2.clone(), Arc::new(registry)).with_domain_tools(domain.clone()))
            }
        }),
    );
    sup.init_trace_persistence();
    for id in &world.agents {
        sup.spawn_agent(id);
    }

    let mut names: std::collections::HashMap<String, String> = Default::default();
    for id in &world.agents {
        let n = s
            .get_agent(id)
            .await
            .ok()
            .flatten()
            .map(|a| a.name)
            .unwrap_or_else(|| id.clone());
        names.insert(id.clone(), n);
    }

    let started = std::time::Instant::now();
    let env_id = world.env.environment_id().to_string();

    // Draw on the ALTERNATE SCREEN (like htop): redrawing never scrolls
    // the user's real terminal or floods its scrollback. Ctrl+C exits
    // cleanly too.
    print!("\x1b[?1049h\x1b[2J");
    use std::io::Write;
    let _ = std::io::stdout().flush();
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            print!("\x1b[?1049l");
            let _ = std::io::stdout().flush();
            std::process::exit(0);
        }
    });

    let mut last_msg_id = 0i64;
    let mut last_change_id = 0i64;
    let mut feed: Vec<(String, String)> = Vec::new(); // (speaker, line)
    let mut deal_price: Option<f64> = None;

    loop {
        // ---- gather data
            for m in s
            .messages_in_world(&env_id)
            .await
            .unwrap_or_default()
        {
            if m.id > last_msg_id {
                last_msg_id = m.id;
                let from = s
                    .get_agent(&m.sender)
                    .await
                    .ok()
                    .flatten()
                    .map(|a| a.name)
                    .unwrap_or_else(|| m.sender.clone());
                let to = s
                    .get_agent(&m.recipient)
                    .await
                    .ok()
                    .flatten()
                    .map(|a| a.name)
                    .unwrap_or_else(|| m.recipient.clone());
                feed.push((from.clone(), format!("→ {to}: {}", m.body)));
            }
        }
        for r in s
            .list_changes(&env_id, None, 50)
            .await
            .unwrap_or_default()
            .into_iter()
            .rev()
        {
            if r.id <= last_change_id {
                continue;
            }
            last_change_id = r.id;
            let p: serde_json::Value = serde_json::from_str(&r.payload).unwrap_or_default();
            match r.kind.as_str() {
                "offer_made" => {
                    let from = p.get("from").and_then(|v| v.as_str()).unwrap_or("");
                    let who = names.get(from).cloned().unwrap_or_else(|| from.to_string());
                    feed.push((who.clone(), format!("offered ${:.0}", p.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0))));
                }
                "deal" => {
                    deal_price = p.get("price").and_then(|v| v.as_f64());
                    feed.push(("market".into(), format!("🤝 deal closed at ${:.0}", deal_price.unwrap_or(0.0))));
                }
                _ => {}
            }
        }
        if feed.len() > 400 {
            feed = feed.split_off(feed.len() - 400);
        }

        // ---- board data
        let board = world.env.board_snapshot().await;
        // Live bid/ask: the negotiation prices live in the WORDS — parse
        // each party's latest dollar mention from the transcript (the
        // formal ledger offers serve as fallback).
        let formal: Vec<(String, f64)> = s
            .list_changes(&env_id, Some("offer_made"), 10)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|c| {
                let p: serde_json::Value = serde_json::from_str(&c.payload).ok()?;
                let who = p.get("from").and_then(|v| v.as_str()).and_then(|f| names.get(f).cloned())?;
                Some((who, p.get("price").and_then(|v| v.as_f64())?))
            })
            .collect();
        let mut convo: Vec<(String, String)> = s
            .messages_in_world(&env_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|m| {
                let who = names.get(&m.sender).cloned()?;
                Some((who, m.body))
            })
            .collect();
        convo.extend(feed_offers(&names, &s, &env_id).await);

        let latest_price = |party: &str| -> Option<f64> {
            for (who, said) in convo.iter().rev() {
                if who != party {
                    continue;
                }
                if said.to_lowercase().contains("budget") {
                    continue; // "my budget is $100" is not a bid
                }
                if let Some(p) = last_dollar(said) {
                    return Some(p);
                }
            }
            formal.iter().rev().find(|(w, _)| w == party).map(|(_, p)| *p)
        };
        let buyer_bid = latest_price("buyer");
        let seller_ask = latest_price("seller");

        let mut states = Vec::new();
        for (id, name) in &names {
            let st = s.get_state(id).await.ok().flatten();
            let status = match st.as_ref().map(|x| x.status.as_str()) {
                Some("thinking") => "thinking",
                Some("sleeping") => "sleeping",
                Some("idle") => "idle",
                Some("stopped") => "stopped",
                _ => "…",
            };
            states.push((name.clone(), status.to_string()));
        }

        // ---- render
        // Home cursor, overwrite the frame, clear whatever hangs below
        // it — the frame never overflows the bottom row, so the terminal
        // never scrolls and no copies pile up.
        let height = 24usize;
        let mut left: Vec<String> = Vec::new();
        let mut prev_speaker: Option<&String> = None;
        for (who, line) in &feed {
            if prev_speaker != Some(who) {
                left.push(String::new()); // space between each side
                prev_speaker = Some(who);
            }
            let wrapped = wrap(line, W_LEFT - 3);
            for (i, l) in wrapped.into_iter().enumerate() {
                left.push(if i == 0 { l } else { format!("  {l}") });
            }
        }
        // Live activity: whoever is mid-think shows as dim text pinned to
        // the bottom of the feed — the words land when the think lands,
        // but the work is visible now.
        let thinkers: Vec<String> = states
            .iter()
            .filter(|(_, st)| st == "thinking")
            .map(|(n, _)| format!("  \x1b[2m· {n} is thinking…\x1b[0m"))
            .collect();
        let room = height - 4 - thinkers.len();
        let mut left_show: Vec<String> =
            left[left.len().saturating_sub(room)..].to_vec();
        left_show.extend(thinkers);

        let mut out = String::from("\x1b[H\x1b[J");
        out.push_str("TRADER MARKET");
        out.push_str(&" ".repeat(W_LEFT - 13));
        out.push_str("│  BOARD\n");
        out.push_str(&"─".repeat(W_LEFT));
        out.push_str("┼");
        out.push_str(&"─".repeat(W_RIGHT));
        out.push('\n');

        let mut right: Vec<String> = Vec::new();
        right.push("MARKET".to_string());
        let item_rows: Vec<(String, String)> = board
            .iter()
            .filter_map(|(k, v)| {
                let id = k.strip_prefix("item:")?;
                let status = v.get("status").and_then(|s| s.as_str())?.to_string();
                Some((id.to_string(), status))
            })
            .collect();
        if item_rows.is_empty() {
            right.push("  (nothing listed yet)".into());
        }
        for (id, status) in &item_rows {
            right.push(format!("  item:{} — {}", &id[..8.min(id.len())], status));
        }
        right.push(format!(
            "  buyer bid: {}",
            buyer_bid.map(|p| format!("${p:.0}")).unwrap_or_else(|| "—".into())
        ));
        right.push(format!(
            "  seller ask: {}",
            seller_ask.map(|p| format!("${p:.0}")).unwrap_or_else(|| "—".into())
        ));
        right.push(format!(
            "  deal: {}",
            deal_price.map(|p| format!("${p:.0}")).unwrap_or_else(|| "negotiating…".into())
        ));
        right.push(String::new());
        right.push("AGENTS".to_string());
        for (name, status) in &states {
            right.push(format!("  {name}: {status}"));
        }

        let rows = height.saturating_sub(3);
        for i in 0..rows {
            let l = left_show.get(i).cloned().unwrap_or_default();
            let r = right.get(i).cloned().unwrap_or_default();
            out.push_str(&pad(&clip(&l, W_LEFT - 1), W_LEFT));
            out.push_str("│ ");
            out.push_str(&clip(&r, W_RIGHT - 2));
            if i + 1 < rows {
                out.push('\n');
            }
        }
        out.push_str("\x1b[J"); // clear anything below the frame — no scroll

        print!("{out}");
        let _ = std::io::stdout().flush();

        if deal_price.is_some() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            break;
        }
        if started.elapsed() > Duration::from_secs(run_secs) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    for id in &world.agents {
        if let Some(h) = sup.get(id) {
            h.stop();
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Leave the alternate screen BEFORE printing the closing line — it
    // lands in the user's real terminal, not the discarded buffer.
    print!("\x1b[?1049l");
    let _ = std::io::stdout().flush();
    println!("\n(done — world rows are in {})", dir.display());
}
