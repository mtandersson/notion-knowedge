//! Exact vector-only model baseline. No judgments, ANN approximation, or network.
use notion_knowledge_core::embedding::{self, EmbeddingProvider};
use notion_knowledge_retrieval::qwen::{QwenConfig, QwenProvider};
use serde::Deserialize;
use serde_json::json;
use std::{
    io::{Read, Write},
    time::Instant,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema_version: u32,
    dataset_id: String,
    as_of: String,
    mode: String,
    top_k: usize,
    pages: Vec<Page>,
    queries: Vec<Query>,
}
#[derive(Deserialize)]
struct Page {
    title: String,
    status: String,
    updated_at: String,
    links: Vec<String>,
    chunks: Vec<Chunk>,
}
#[derive(Deserialize)]
struct Chunk {
    id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    id: String,
    text: String,
    language: String,
    category: String,
}

fn rank<'a>(
    ids: &'a [String],
    vectors: &[Vec<f32>],
    query: &[f32],
    top_k: usize,
) -> Result<Vec<&'a String>, Box<dyn std::error::Error>> {
    if ids.len() != vectors.len()
        || query.is_empty()
        || vectors.iter().any(|v| v.len() != query.len())
    {
        return Err("vector shape mismatch".into());
    }
    let mut scores: Vec<_> = vectors
        .iter()
        .enumerate()
        .map(|(index, vector)| {
            (
                index,
                vector.iter().zip(query).map(|(a, b)| a * b).sum::<f32>(),
            )
        })
        .collect();
    if scores.iter().any(|(_, score)| !score.is_finite()) {
        return Err("nonfinite score".into());
    }
    scores.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| ids[a.0].cmp(&ids[b.0])));
    Ok(scores
        .iter()
        .take(top_k)
        .map(|(index, _)| &ids[*index])
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let assets = args
        .next()
        .ok_or("usage: qwen-eval ASSETS MEASUREMENTS_JSON")?;
    let measurements = args.next().ok_or("missing measurements path")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let mut input = String::new();
    std::io::stdin()
        .take(8 * 1024 * 1024 + 1)
        .read_to_string(&mut input)?;
    if input.len() > 8 * 1024 * 1024 {
        return Err("request too large".into());
    }
    let request: Request = serde_json::from_str(&input)?;
    if request.schema_version != 1
        || request.mode != "vector"
        || request.top_k == 0
        || request.pages.is_empty()
        || request.queries.is_empty()
    {
        return Err("unsupported or empty evaluation request".into());
    }
    let mut ids = Vec::new();
    let mut documents = Vec::new();
    for page in &request.pages {
        for chunk in &page.chunks {
            ids.push(chunk.id.clone());
            documents.push(format!(
                "Title: {}\nStatus: {}\nUpdated: {}\nLinks: {}\n{}",
                page.title,
                page.status,
                page.updated_at,
                page.links.join(", "),
                chunk.text
            ));
        }
    }
    let started = Instant::now();
    let provider = QwenProvider::load(QwenConfig::cpu(assets)).await?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;
    let started = Instant::now();
    // Keep each call within the provider's admission limits, independently of corpus size.
    let mut vectors = Vec::new();
    for document in &documents {
        vectors.extend(embedding::embed_batch(&provider, std::slice::from_ref(document)).await?);
    }
    let corpus_embed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut results = Vec::new();
    let mut timings = Vec::new();
    for query in &request.queries {
        let started = Instant::now();
        let query_vectors =
            embedding::embed_batch(&provider, std::slice::from_ref(&query.text)).await?;
        let embedding_ms = started.elapsed().as_secs_f64() * 1000.0;
        let started = Instant::now();
        let ranked = rank(&ids, &vectors, &query_vectors[0], request.top_k)?;
        let search_ms = started.elapsed().as_secs_f64() * 1000.0;
        results.push(json!({"query_id":query.id,"ranked_source_ids":ranked}));
        timings.push(
            json!({"query_id":query.id,"language":query.language,"category":query.category,
            "embedding_ms":embedding_ms,"exact_search_ms":search_ms}),
        );
    }
    let record = json!({"schema_version":1,"dataset_id":request.dataset_id,"as_of":request.as_of,
        "model":provider.metadata(),"backend":"exhaustive cosine over normalized Qwen vectors",
        "batch_size":1,"max_tokens":512,"query_instruction":null,
        "document_format":"Title: {title}\\nStatus: {status}\\nUpdated: {updated_at}\\nLinks: {comma-separated link IDs}\\n{chunk text}",
        "chunks":ids.len(),"model_load_ms":load_ms,"corpus_embed_ms":corpus_embed_ms,"queries":timings});
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(measurements)?;
    serde_json::to_writer_pretty(&mut file, &record)?;
    file.write_all(b"\n")?;
    serde_json::to_writer(
        std::io::stdout(),
        &json!({"schema_version":1,"dataset_id":request.dataset_id,
        "mode":"vector","results":results}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_similarity_orders_hits_and_breaks_ties_by_identity() {
        let ids = ["z", "a", "other"].map(str::to_owned);
        let vectors = vec![vec![1., 0.], vec![1., 0.], vec![0., 1.]];
        assert_eq!(
            rank(&ids, &vectors, &[1., 0.], 2).unwrap(),
            vec![&ids[1], &ids[0]]
        );
        assert_eq!(rank(&ids, &vectors, &[0., 1.], 1).unwrap(), vec![&ids[2]]);
        assert!(rank(&ids, &vectors, &[1.], 2).is_err());
        assert!(rank(&ids, &vectors, &[f32::NAN, 0.], 2).is_err());
    }

    #[test]
    fn adapter_query_contract_rejects_ground_truth() {
        let query = json!({"id":"q","text":"question","language":"en","category":"semantic"});
        assert!(serde_json::from_value::<Query>(query.clone()).is_ok());
        let mut leaked = query;
        leaked["relevant"] = json!([{"source_id":"answer","grade":3}]);
        assert!(serde_json::from_value::<Query>(leaked).is_err());
    }
}
