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
use agenthub_runtime::{DomainCtx, DomainTool, Scene, SceneWorld};
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
impl DomainTool for SendOffer {
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

    async fn call(&self, args: &Value, ctx: &DomainCtx<'_>) -> Result<Value, String> {
        let to = args.get("to").and_then(Value::as_str).ok_or("missing 'to'")?;
        let item = args.get("item").and_then(Value::as_str).ok_or("missing 'item'")?;
        let price = args.get("price").and_then(Value::as_f64).ok_or("missing 'price'")?;
        if !price.is_finite() || price <= 0.0 {
            return Err(format!("invalid price {price}"));
        }
        // The item must exist in this world.
        if ctx.env.board_get(&format!("item:{item}")).await.is_none() {
            return Err(format!("unknown item '{item}'"));
        }
        let to_agent = ctx
            .store
            .get_agent(to)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no agent '{to}'"))?;

        let payload = json!({
            "item": item, "from": ctx.actor, "to": to_agent.id, "price": price
        });
        ctx.store
            .append_change(&ctx.env.environment_id().to_string(), Some(ctx.actor), "offer_made", &payload.to_string())
            .await
            .map_err(|e| e.to_string())?;

        // The offer travels as a task; the board never holds offers.
        ctx.store
            .enqueue_task(
                &to_agent.id,
                &format!("offer: {item} @ ${price:.0}"),
                &format!(
                    "You were offered ${price:.0} for the {item} by {actor}. \
                     Use send_offer to counter, or accept_deal to close.",
                    price = price,
                    item = item,
                    actor = ctx.actor,
                ),
            )
            .await
            .map_err(|e| e.to_string())?;

        // Item is in live negotiation.
        ctx.env
            .set_board(&format!("item:{item}"), &json!({"status": "pending", "last_price": price}))
            .await;
        Ok(json!({ "ok": true, "offered": price }))
    }
}

/// `accept_deal(with, item, price)` — close the deal: ledger row, item
/// sold, both sides' queues settle.
pub struct AcceptDeal;

#[async_trait]
impl DomainTool for AcceptDeal {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "accept_deal".into(),
            description: "Accept a price with another agent and close the deal.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "with":  { "type": "string", "description": "counterpart agent name" },
                    "item":  { "type": "string" },
                    "price": { "type": "number" }
                },
                "required": ["with", "item", "price"]
            }),
        }
    }

    async fn call(&self, args: &Value, ctx: &DomainCtx<'_>) -> Result<Value, String> {
        let with = args.get("with").and_then(Value::as_str).ok_or("missing 'with'")?;
        let item = args.get("item").and_then(Value::as_str).ok_or("missing 'item'")?;
        let price = args.get("price").and_then(Value::as_f64).ok_or("missing 'price'")?;
        if !price.is_finite() || price <= 0.0 {
            return Err(format!("invalid price {price}"));
        }
        let other = ctx
            .store
            .get_agent(with)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no agent '{with}'"))?;

        // Ledger: the truth.
        let payload = json!({
            "item": item, "party_a": ctx.actor, "party_b": other.id, "price": price
        });
        ctx.store
            .append_change(&ctx.env.environment_id().to_string(), Some(ctx.actor), "deal", &payload.to_string())
            .await
            .map_err(|e| e.to_string())?;

        // Board projection: item is gone. This write announces the sale to
        // the item's subscribers (e.g. waking the buyer to go take it).
        ctx.env
            .set_board(&format!("item:{item}"), &json!({ "status": "sold", "last_price": price }))
            .await;

        // A closed deal ends the follow-up: both parties drop their
        // subscription — the announcement above was already delivered.
        let item_key = format!("item:{item}");
        let _ = ctx.store.unsubscribe(&ctx.actor, &item_key).await;
        let _ = ctx.store.unsubscribe(&other.id, &item_key).await;

        // Notify the counterpart (their loop will think + settle).
        ctx.store
            .enqueue_task(
                &other.id,
                &format!("deal closed: {item} @ ${price:.0}"),
                &format!(
                    "{actor} closed the deal at ${price:.0} for the {item}. \
                     Nothing to do — remember it and rest.",
                    actor = ctx.actor,
                    price = price,
                    item = item,
                ),
            )
            .await
            .map_err(|e| e.to_string())?;

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

        let env = agenthub_runtime::EnvHandle::new(store.clone(), env.id);

        // Verbs: register + grant to both roles.
        for t in [&SendOffer as &dyn DomainTool, &AcceptDeal as &dyn DomainTool] {
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

    fn domain_tools(&self) -> Vec<Arc<dyn DomainTool>> {
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
