//! Trader scene: one buyer, one seller, one item. Negotiation happens
//! agent-to-agent; the world only records rules, item status, and deals.
//!
//! Board keys: `item:{name}` → { "status": "available"|"pending"|"sold",
//! "last_price": number|null }. Offers never touch the board — they travel
//! agent-to-agent as tasks, and every mutation lands in the ledger.

use std::sync::Arc;

use agenthub_providers::ToolSpec;
use agenthub_runtime::{DomainCtx, DomainTool, Scene, SceneWorld};
use agenthub_storage::{NewAgent, Store};
use async_trait::async_trait;
use serde_json::{json, Value};

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

        // Board projection: item is gone.
        ctx.env
            .set_board(&format!("item:{item}"), &json!({ "status": "sold", "last_price": price }))
            .await;

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

        // Roster: souls carry the private constraints (budget / floor).
        let buyer = agent(store, BUYER, "You are a buyer. You have a budget of $100. \
             Get the item as cheap as possible, but a fair deal today beats no deal.")
            .await?;
        let seller = agent(store, SELLER, "You are a seller. You must clear at least $70 for the item. \
             Squeeze the best price, but close the deal today.")
            .await?;

        // Catalog.
        store
            .set_env_state(&env.id, &format!("item:{ITEM}"), &json!({"status": "available", "last_price": null}).to_string())
            .await
            .map_err(|e| e.to_string())?;

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

        // Seeds: they name the counterpart, so each side knows who to talk to.
        store
            .enqueue_task(
                &buyer,
                "buy the watch",
                "The watch is on the board. The seller agent is named 'seller'. \
                 Your FIRST action must be send_offer(to: \"seller\", item: \"watch\", \
                 price: <your opening offer>) — do not sleep or wait instead. \
                 Your budget is $100; keep countering until a deal is closed with \
                 accept_deal (then remember it) or you choose to walk away.",
            )
            .await
            .map_err(|e| e.to_string())?;
        store
            .enqueue_task(
                &seller,
                "sell the watch",
                "You hold the watch. The buyer agent is named 'buyer' — they will send \
                 you offers as tasks; respond with send_offer (counter) or accept_deal \
                 (close). You need at least $70.",
            )
            .await
            .map_err(|e| e.to_string())?;

        Ok(SceneWorld {
            env: agenthub_runtime::EnvHandle::new(store.clone(), env.id),
            agents: vec![buyer, seller],
        })
    }

    fn domain_tools(&self) -> Vec<Arc<dyn DomainTool>> {
        vec![Arc::new(SendOffer), Arc::new(AcceptDeal)]
    }
}

async fn agent(store: &Arc<Store>, name: &str, soul: &str) -> Result<String, String> {
    let a = store
        .create_agent(NewAgent {
            name: name.to_string(),
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
