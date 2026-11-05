//! Trader scene: one buyer, one seller, one item — a thin implementation
//! of the base. The world runs through **conversation**: the buyer finds
//! the seller on the roster, approaches them (`talk_to`), they negotiate,
//! and `send_offer`/`accept_deal` are the binding handshake both sides
//! must use to close (the ledger is the truth).
//!
//! Ownership is private: the seller *possesses* the watch (its soul and
//! memory carry that), and the buyer's budget lives only in the buyer.
//! The board is a projection for **human auditing** of item status —
//! never read by agents, never announced to them.

use std::sync::Arc;

use agenthub_providers::ToolSpec;
use agenthub_runtime::{AgentTools, Scene, SceneWorld, Tool};
use agenthub_storage::{NewAgent, Store};
use async_trait::async_trait;
use serde_json::{json, Value};

/// The item this world trades. Public so tests and humans can reference it;
/// agents know it from their souls, never from a board.
pub const ITEM: &str = "watch";
const BUYER: &str = "buyer";
const SELLER: &str = "seller";

// ---------------------------------------------------------------- tools

/// `send_offer(to, item, price)` — counter or open. Enqueues a response
/// task on the counterpart's queue, flips the item to pending, ledgers it.
pub struct SendOffer;

#[async_trait]
impl Tool for SendOffer {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "send_offer".into(),
            description: "Make or counter an offer to another agent for an item."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "to":    { "type": "string", "description": "agent name" },
                    "item":  { "type": "string" },
                    "price": { "type": "number" }
                },
                "required": ["to", "item", "price"]
            }),
        }
    }

    fn describe(&self, result: &Value) -> String {
        let err = result.get("error").and_then(Value::as_str)
            .map(|e| format!(" (failed: {e})")).unwrap_or_default();
        format!("offered ${:.0}{err}", result.get("offered").and_then(Value::as_f64).unwrap_or(0.0))
    }

    async fn call(&self, args: &Value, ctx: &AgentTools) -> Result<Value, String> {
        let to = args.get("to").and_then(Value::as_str).ok_or("missing 'to'")?;
        let item = args.get("item").and_then(Value::as_str).ok_or("missing 'item'")?;
        let price = args.get("price").and_then(Value::as_f64).ok_or("missing 'price'")?;
        if !price.is_finite() || price <= 0.0 {
            return Err(format!("invalid price {price}"));
        }
        // Items are private (souls + memory), not board facts — no
        // existence check; ownership is trust + house rules.
        let to_agent = ctx
            .store()
            .get_agent(to)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no agent '{to}'"))?;

        let payload = json!({
            "item": item, "from": ctx.agent_id(), "to": to_agent.id, "price": price
        });
        ctx.store()
            .append_change(&ctx.env().environment_id().to_string(), Some(ctx.agent_id()), "offer_made", &payload.to_string())
            .await
            .map_err(|e| e.to_string())?;

        // Fact only: the ledger row. The words ride talk_to in the same
        // breath — one conversation, no duplicate words or task rows.
        Ok(json!({ "ok": true, "offered": price }))
    }
}

/// `accept_deal(with, item, price)` — close the deal: ledger row, item
/// sold, both sides' queues settle.
pub struct AcceptDeal;

#[async_trait]
impl Tool for AcceptDeal {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "accept_deal".into(),
            description: "Accept a price with another agent and close the deal.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "with":   { "type": "string", "description": "counterpart agent name" },
                    "item":   { "type": "string" },
                    "price":  { "type": "number" },
                    "i_give": { "type": "string", "enum": ["money", "item"],
                                "description": "what YOU hand over: money (you're buying) or item (you're selling) — per your own soul" }
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
        let item = args.get("item").and_then(Value::as_str).ok_or("missing 'item'")?;
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

        // THE transfer, atomically (base primitive). The tool is STATELESS:
        // the caller declares what THEY give (their soul knows who they
        // are); the counterpart gives the complement. Nothing is inferred.
        let i_give = args.get("i_give").and_then(Value::as_str).ok_or("missing 'i_give' (money or item)?")?;
        let (i_give_kind, i_give_name, i_give_qty, their_kind, their_name, their_qty) = match i_give {
            "money" => ("money", "usd", price, "item", item, 1.0),
            "item" => ("item", item, 1.0, "money", "usd", price),
            other => return Err(format!("i_give must be 'money' or 'item', got '{other}'")),
        };
        let my_give = agenthub_storage::store::agent::AssetGive {
            agent_id: ctx.agent_id().to_string(),
            kind: i_give_kind.into(),
            name: i_give_name.into(),
            qty: i_give_qty,
        };
        let their_give = agenthub_storage::store::agent::AssetGive {
            agent_id: other.id.clone(),
            kind: their_kind.into(),
            name: their_name.into(),
            qty: their_qty,
        };
        // Idempotence guard: if the watch already sold, refuse — a repeated
        // accept_deal must not run the trade backwards.
        let already_sold = ctx
            .env()
            .board_get(&format!("item:{item}"))
            .await
            .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(|s| s == "sold"))
            .unwrap_or(false);
        if already_sold {
            return Err(format!("the {item} is already sold — nothing to accept"));
        }
        let payload = json!({
            "item": item, "party_a": ctx.agent_id(), "party_b": other.id, "price": price
        });
        ctx.store()
            .exchange(
                &ctx.env().environment_id().to_string(),
                Some(ctx.agent_id()),
                "deal",
                &payload.to_string(),
                my_give,
                their_give,
            )
            .await
            .map_err(|e| e.to_string())?;

        // Board projection (human audit only).
        ctx.env()
            .set_board(&format!("item:{item}"), &json!({ "status": "sold", "last_price": price }))
            .await;

        // The thread's lifecycle is the AGENTS' choice (close_thread /
        // leave_thread) — the deal is a fact, not a conversation-ender.
        Ok(json!({ "ok": true, "deal_price": price }))
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

        // Roster: souls carry the private facts (who owns what, budget,
        // floor). Nothing is placed on the public board — the seller owns
        // the watch, the buyer wants one, and they find each other through
        // the roster and conversation.
        let buyer = agent(
            store,
            BUYER,
            "buyer",
            "You are a buyer. You want to buy a watch. You have a budget of $100 — \
             get it as cheap as possible, but a fair deal today beats no deal. \
             Check who_is_here, find someone who sells, and approach them with \
             talk_to: greet them, ask what they have, negotiate. Close with the \
             send_offer/accept_deal handshake.",
        )
        .await?;
        let seller = agent(
            store,
            SELLER,
            "seller",
            "You are a seller. You own a watch — remember it; it is yours to sell. \
             You must clear at least $70, squeeze the best price, and close today. \
             Entertain buyers who approach you (talk_to). If no one approaches, go \
             look for buyers: check who_is_here and approach them with a pitch. \
             Close with the send_offer/accept_deal handshake.",
        )
        .await?;

        // Ownership, as base asset rows (the REAL state): the buyer holds
        // $100, the seller holds the watch. Souls describe it; rows prove it.
        store
            .adjust_asset(&buyer, "money", "usd", 100.0)
            .await
            .map_err(|e| e.to_string())?;
        store
            .adjust_asset(&seller, "item", ITEM, 1.0)
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
