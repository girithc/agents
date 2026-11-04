//! World tests. The negotiation test is a REAL multi-agent negotiation
//! through api.meta.ai — buyer and seller trade genuinely, no script.
//! Skips when MODEL_API_KEY is not set (env or ./.env).

use std::sync::Arc;
use std::time::Duration;

use agenthub_providers::meta::MetaProvider;
use agenthub_providers::registry::Registry;
use agenthub_runtime::{LlmBrain, Scene, Supervisor};
use agenthub_storage::{Store, Vault};

use crate::trader::TraderScene;

async fn store() -> Arc<Store> {
    let dir = std::env::temp_dir().join(format!("agenthub-test-{}", uuid::Uuid::new_v4()));
    Arc::new(
        Store::open(&dir.join("test.db"), Vault::open(&dir.join("master.key")).unwrap())
            .expect("open store"),
    )
}

fn register_key(s: &Store) -> bool {
    match agenthub_runtime::model_api_key() {
        Some(k) => futures::executor::block_on(s.put_secret("meta.api_key", &k)).is_ok(),
        None => false,
    }
}

#[tokio::test]
async fn scene_setup_is_complete() {
    let s = store().await;
    let scene = TraderScene;
    let world = scene.setup(&s).await.unwrap();

    assert_eq!(world.agents.len(), 2);
    assert_eq!(scene.name(), "trader");
    let specs: Vec<_> = scene.domain_tools().iter().map(|t| t.spec().name).collect();
    assert_eq!(specs, vec!["send_offer", "accept_deal"]);

    // Catalog on the board, rules in the world, seeds queued.
    assert_eq!(world.env.board_get("item:watch").await.unwrap()["status"], "available");
    let rules = world.env.rules_for("nobody").await;
    assert_eq!(rules.len(), 2);
    for id in &world.agents {
        assert_eq!(s.list_tasks(id, None).await.unwrap().len(), 1);
    }
}

/// Live multi-agent negotiation: buyer (budget $100) vs seller (floor $70).
/// The deal must land inside both constraints — a semantic acceptance test
/// of the whole stack, not a script.
#[tokio::test]
async fn trader_world_negotiates_to_a_real_deal() {
    let s = store().await;
    if !register_key(&s) {
        eprintln!("skipping: MODEL_API_KEY not set");
        return;
    }

    let scene = TraderScene;
    let world = scene.setup(&s).await.unwrap();

    let sup = Supervisor::new(
        s.clone(),
        world.env.clone(),
        Arc::new({
            let s2 = s.clone();
            let domain = scene.domain_tools();
            move |_agent| {
                let registry = Registry::new();
                registry.register(Arc::new(MetaProvider::new(None)));
                Arc::new(
                    LlmBrain::new(s2.clone(), Arc::new(registry))
                        .with_domain_tools(domain.clone()),
                )
            }
        }),
    );
    for id in &world.agents {
        sup.spawn_agent(id);
    }

    // Wait for the deal to land in the ledger.
    let mut deal_payload: Option<serde_json::Value> = None;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(300) {
        let changes = s
            .list_changes(world.env.environment_id(), Some("deal"), 1)
            .await
            .unwrap_or_default();
        if let Some(latest) = changes.first() {
            deal_payload = Some(serde_json::from_str(&latest.payload).unwrap_or_default());
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let payload = deal_payload.expect("negotiation should produce a deal within 300s");

    // Semantic checks: the deal respects both souls' constraints.
    let price = payload["price"].as_f64().expect("deal has a price");
    assert!(
        (70.0..=100.0).contains(&price),
        "deal price {price} must be within seller floor ($70) and buyer budget ($100)"
    );

    // Board projection: item sold at the agreed price.
    let board = world.env.board_get("item:watch").await.unwrap();
    assert_eq!(board["status"], "sold");
    assert_eq!(board["last_price"], price);

    for id in &world.agents {
        sup.get(id).unwrap().stop();
        sup.get(id).unwrap().finished().await;
    }
}
