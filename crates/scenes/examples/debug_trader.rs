// temporary debug harness
use agenthub_runtime::Scene;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let dir = std::env::temp_dir().join(format!("agenthub-dbg-{}", uuid::Uuid::new_v4()));
    let s = Arc::new(
        agenthub_storage::Store::open(&dir.join("test.db"), agenthub_storage::Vault::open(&dir.join("master.key")).unwrap()).unwrap(),
    );
    s.put_secret("meta.api_key", &agenthub_runtime::model_api_key().unwrap()).await.unwrap();

    let scene = agenthub_scenes::trader::TraderScene;
    let world = scene.setup(&s).await.unwrap();

    let sup = agenthub_runtime::Supervisor::new(
        s.clone(),
        world.env.clone(),
        Arc::new({
            let s2 = s.clone();
            let domain = scene.domain_tools();
            move |_a| {
                let registry = agenthub_providers::registry::Registry::new();
                registry.register(Arc::new(agenthub_providers::meta::MetaProvider::new(None)));
                Arc::new(agenthub_runtime::LlmBrain::new(s2.clone(), Arc::new(registry)).with_domain_tools(domain.clone()))
            }
        }),
    );
    sup.init_trace_persistence();
    for id in &world.agents { sup.spawn_agent(id); }

    tokio::time::sleep(Duration::from_secs(90)).await;

    println!("=== LEDGER ===");
    for c in s.list_changes(world.env.environment_id(), None, 20).await.unwrap() {
        println!("  {} actor={:?} {} {}", c.id, c.actor, c.kind, c.payload);
    }
    println!("=== BOARD ===");
    for (k, v) in world.env.board_snapshot().await { println!("  {k} = {v}"); }
    for id in &world.agents {
        println!("=== AGENT {id} ===");
        let st = s.get_state(id).await.unwrap(); println!("  state = {st:?}");
        for t in s.list_tasks(id, None).await.unwrap() { println!("  task {} [{}] {} :: {}", &t.id[..8], t.status.as_str(), t.title, t.body); }
        for m in s.recent_memory(id, 10).await.unwrap() { println!("  mem: {}", m.content); }
    }
    for id in &world.agents { sup.get(id).unwrap().stop(); }
    tokio::time::sleep(Duration::from_millis(500)).await; // let the writer drain
    println!("=== TRACES (persisted) ===");
    let spans = s.debug_all_spans().await.unwrap_or_default();
    for sp in spans.iter().rev().take(30) {
        let f = sp.fields.clone();
        let tool = f.get("tool").and_then(|t| t.as_str()).unwrap_or("");
        let msg = f.get("message").and_then(|t| t.as_str()).unwrap_or("");
        println!("  span {} {} tool={tool} msg={msg}", sp.name, sp.run_id.as_deref().unwrap_or("-"));
    }
}
