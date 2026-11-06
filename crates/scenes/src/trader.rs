//! Trader scene: one buyer, one seller, TWO watches — a thin
//! implementation of the base. The world runs through **conversation**:
//! the buyer finds the seller on the roster, approaches them (`talk_to`),
//! compares the watches, they negotiate, and `send_offer`/`accept_deal`
//! are the binding handshake both sides must use to close (the ledger is
//! the truth).
//!
//! Possession and constraints are DATA, never prose: the watches live in
//! the environment item registry (serial-numbered, owner field), the
//! seller's floors and the buyer's budget are anchored facts. Souls are
//! character only. The board is a projection for **human auditing** —
//! never read by agents, never announced to them.

use std::sync::Arc;

use agenthub_providers::ToolSpec;
use agenthub_runtime::{AgentTools, Scene, SceneWorld, Tool};
use agenthub_storage::{NewAgent, Store};
use async_trait::async_trait;
use serde_json::{json, Value};

const BUYER: &str = "buyer";
const SELLER: &str = "seller";

// ---------------------------------------------------------------- tools

/// `send_offer(item, price)` — the formal offer for a PARTICULAR item
/// (its id routes the offer to the owner). Ledgers it, marks the item
/// pending; the words ride talk_to in the same breath.
pub struct SendOffer;

#[async_trait]
impl Tool for SendOffer {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "send_offer".into(),
            description: "Make or counter a formal offer for a particular item \
                          (by its item:<id>). The owner is resolved for you."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "item":  { "type": "string", "description": "item:<id> — WHICH particular thing" },
                    "price": { "type": "number" }
                },
                "required": ["item", "price"]
            }),
        }
    }

    fn describe(&self, result: &Value) -> String {
        let err = result.get("error").and_then(Value::as_str)
            .map(|e| format!(" (failed: {e})")).unwrap_or_default();
        format!(
            "offered ${:.0} for {}{err}",
            result.get("offered").and_then(Value::as_f64).unwrap_or(0.0),
            result.get("item").and_then(Value::as_str).unwrap_or("an item"),
        )
    }

    async fn call(&self, args: &Value, ctx: &AgentTools) -> Result<Value, String> {
        let item = args.get("item").and_then(Value::as_str).ok_or("missing 'item' (item:<id>)?")?;
        let price = args.get("price").and_then(Value::as_f64).ok_or("missing 'price'")?;
        if !price.is_finite() || price <= 0.0 {
            return Err(format!("invalid price {price}"));
        }
        // The item's identity routes the offer to its owner.
        let owner = ctx
            .store()
            .item_owner(item)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no item '{item}'"))?
            .ok_or_else(|| format!("item '{item}' is unowned — nothing to negotiate against"))?;
        if owner == ctx.agent_id() {
            return Err("that item is yours — you cannot offer yourself".into());
        }

        let payload = json!({
            "item": item, "from": ctx.agent_id(), "to": owner, "price": price
        });
        ctx.store()
            .append_change(&ctx.env().environment_id().to_string(), Some(ctx.agent_id()), "offer_made", &payload.to_string())
            .await
            .map_err(|e| e.to_string())?;
        ctx.store()
            .set_item_status(item, "pending")
            .await
            .map_err(|e| e.to_string())?;

        // Fact only: the ledger row. The words ride talk_to in the same
        // breath — one conversation, no duplicate words or task rows.
        Ok(json!({ "ok": true, "offered": price, "item": item }))
    }
}

/// `accept_deal(item, price, i_give)` — close the deal on a PARTICULAR
/// item: money and ownership move in one atomic transaction, one ledger
/// row, item marked sold.
pub struct AcceptDeal;

#[async_trait]
impl Tool for AcceptDeal {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "accept_deal".into(),
            description: "Accept a price with another agent and close the deal on a \
                          particular item (by its item:<id>)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "with":   { "type": "string", "description": "counterpart agent name" },
                    "item":   { "type": "string", "description": "item:<id> — WHICH particular thing" },
                    "price":  { "type": "number" },
                    "i_give": { "type": "string", "enum": ["money", "item"],
                                "description": "what YOU hand over: money (you're buying) or item (you're selling) — per your own facts" }
                },
                "required": ["with", "item", "price", "i_give"]
            }),
        }
    }

    fn describe(&self, result: &Value) -> String {
        let err = result.get("error").and_then(Value::as_str)
            .map(|e| format!(" (failed: {e})")).unwrap_or_default();
        format!("deal closed: ${:.0}{err}", result.get("deal_price").and_then(Value::as_f64).unwrap_or(0.0))
    }

    async fn call(&self, args: &Value, ctx: &AgentTools) -> Result<Value, String> {
        let with = args.get("with").and_then(Value::as_str).ok_or("missing 'with'")?;
        let item = args.get("item").and_then(Value::as_str).ok_or("missing 'item' (item:<id>)?")?;
        let price = args.get("price").and_then(Value::as_f64).ok_or("missing 'price'")?;
        if !price.is_finite() || price <= 0.0 {
            return Err(format!("invalid price {price}"));
        }
        let other = ctx
            .store()
            .get_agent(with)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no agent '{with}'"))?;

        // THE transfer, atomically (base primitive). The caller declares
        // what THEY give; the item flips ownership in the same
        // transaction as the money, and only if it's where expected.
        let i_give = args.get("i_give").and_then(Value::as_str).ok_or("missing 'i_give' (money or item)?")?;
        let nothing = || agenthub_storage::store::agent::AssetGive {
            agent_id: String::new(),
            kind: "nothing".into(),
            name: "nothing".into(),
            qty: 0.0,
        };
        let item_owner = match i_give {
            // I pay → the item is currently THEIRS.
            "money" => other.id.clone(),
            // I sell → the item is currently MINE.
            "item" => ctx.agent_id().to_string(),
            other => return Err(format!("i_give must be 'money' or 'item', got '{other}'")),
        };
        let a_give = if i_give == "money" {
            agenthub_storage::store::agent::AssetGive {
                agent_id: ctx.agent_id().to_string(),
                kind: "money".into(),
                name: "usd".into(),
                qty: price,
            }
        } else {
            nothing()
        };
        let b_give = if i_give == "item" {
            agenthub_storage::store::agent::AssetGive {
                agent_id: other.id.clone(),
                kind: "money".into(),
                name: "usd".into(),
                qty: price,
            }
        } else {
            nothing()
        };
        let payload = json!({
            "item": item, "party_a": ctx.agent_id(), "party_b": other.id, "price": price
        });
        ctx.store()
            .exchange(
                &ctx.env().environment_id().to_string(),
                Some(ctx.agent_id()),
                "deal",
                &payload.to_string(),
                a_give,
                b_give,
                Some((item, item_owner.as_str())),
            )
            .await
            .map_err(|e| e.to_string())?;

        // Board projection (human audit only).
        ctx.env()
            .set_board(item, &json!({ "status": "sold", "last_price": price }))
            .await;

        // The thread's lifecycle is the AGENTS' choice (close_thread /
        // leave_thread) — the deal is a fact, not a conversation-ender.
        Ok(json!({ "ok": true, "deal_price": price, "item": item }))
    }
}

// ---------------------------------------------------------------- scene

pub struct TraderScene;

#[async_trait]
impl Scene for TraderScene {
    fn name(&self) -> &str {
        "trader"
    }

    async fn setup(&self, store: &Arc<Store>) -> Result<SceneWorld, String> {
        let env = store.create_environment("trader", 5000).await.map_err(|e| e.to_string())?;

        // House rules — the world's laws.
        store
            .add_rule(&env.id, None, "Be honest about the item and your position.")
            .await
            .map_err(|e| e.to_string())?;
        store
            .add_rule(&env.id, None, "A deal is final only when closed with accept_deal.")
            .await
            .map_err(|e| e.to_string())?;
        store
            .add_rule(&env.id, None, "Either party may leave the negotiation at any time (leave_thread) — the other side is told and may leave too.")
            .await
            .map_err(|e| e.to_string())?;
        store
            .add_rule(&env.id, None, "When a deal is closed (accept_deal succeeded), say goodbye and end the conversation with close_thread.")
            .await
            .map_err(|e| e.to_string())?;

        // Souls: character ONLY — no possession, no numbers. Those are
        // data now (items + facts below).
        let buyer = agent(
            store,
            BUYER,
            "buyer",
            "You are a buyer who values quality and a fair price. Your budget \
             lives in your facts — never exceed it. There may be several \
             things worth buying; ask, compare, and choose deliberately. \
             Find sellers via who_is_here, approach with talk_to: greet, ask \
             what they have, negotiate. Close with the send_offer/\
             accept_deal handshake, and keep your facts current as deals move.",
        )
        .await?;
        let seller = agent(
            store,
            SELLER,
            "seller",
            "You are a seller — you make your living moving stock at good \
             prices. Your floors live in your facts, anchored per item: \
             never sell below them. Entertain buyers who approach you \
             (talk_to); if no one approaches, go look for buyers: check \
             who_is_here and pitch. Close with the send_offer/accept_deal \
             handshake, and keep your facts current as deals move.",
        )
        .await?;

        // Data seeds: the world's stock and the private mandates.
        let watch_a = store
            .mint_item(&env.id, "item", "watch", Some(&seller))
            .await
            .map_err(|e| e.to_string())?;
        let watch_b = store
            .mint_item(&env.id, "item", "watch", Some(&seller))
            .await
            .map_err(|e| e.to_string())?;
        // The seller's mandates — private, anchored per item.
        store.fact_put(&seller, "floor", "$70", &format!("item:{watch_a}")).await.map_err(|e| e.to_string())?;
        store.fact_put(&seller, "floor", "$65", &format!("item:{watch_b}")).await.map_err(|e| e.to_string())?;
        store.fact_put(&seller, "target", "$90", &format!("item:{watch_a}")).await.map_err(|e| e.to_string())?;
        // The buyer's constraint — private.
        store.fact_put(&buyer, "budget", "$100", "").await.map_err(|e| e.to_string())?;
        // Money: the only fungible.
        store
            .adjust_asset(&buyer, "money", "usd", 100.0)
            .await
            .map_err(|e| e.to_string())?;

        let env = agenthub_runtime::EnvHandle::new(store.clone(), env.id);

        // Verbs: register + grant to both roles.
        for t in [&SendOffer as &dyn Tool, &AcceptDeal as &dyn Tool] {
            let spec = t.spec();
            store
                .register_tool(&spec.name, agenthub_storage::store::agent::ToolKind::Builtin, "{}")
                .await
                .map_err(|e| e.to_string())?;
            for a in [&buyer, &seller] {
                store.grant_tool(a, &spec.name).await.map_err(|e| e.to_string())?;
            }
        }

        Ok(SceneWorld {
            env,
            agents: vec![buyer, seller],
        })
    }

    fn domain_tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(SendOffer), Arc::new(AcceptDeal)]
    }
}


async fn agent(store: &Arc<Store>, name: &str, role: &str, soul: &str) -> Result<String, String> {
    let a = store
        .create_agent(NewAgent {
            name: name.to_string(),
            role: role.to_string(),
            system_prompt: soul.to_string(),
            provider: "meta".into(),
            model: "muse-spark-1.3-contributor".into(),
            temperature: 0.5,
            max_output_tokens: 512,
            max_iterations: 6,
            reasoning_effort: "low".into(),
        })
        .await
        .map_err(|e| e.to_string())?;
    store
        .upsert_soul(&a.id, soul, "Do your job well and close your deals today.")
        .await
        .map_err(|e| e.to_string())?;
    Ok(a.id)
}
