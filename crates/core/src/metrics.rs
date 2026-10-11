//! Low-cardinality, payload-free in-process counters and Prometheus text output.
//!
//! No filename, page ID, query, token, URL or caller-supplied label is accepted.
use std::{
    array,
    fmt::Write as _,
    sync::{OnceLock, atomic::{AtomicI64, AtomicU64, Ordering}},
    time::Duration,
};
use crate::logging::{Operation,Outcome};

#[derive(Debug,Clone,Copy)]
pub enum SearchMode {
    Lexical,
    Semantic,
    Hybrid,
}
impl SearchMode {
    fn index(self)->usize {match self {Self::Lexical=>0,Self::Semantic=>1,Self::Hybrid=>2}}
    fn label(self)->&'static str{match self{Self::Lexical=>"lexical",Self::Semantic=>"semantic",Self::Hybrid=>"hybrid"}}
}
fn outcome_index(outcome:Outcome)->usize{
    match outcome {Outcome::Completed|Outcome::Success=>0,Outcome::Rejected=>1,Outcome::Failed=>2}
}
fn label(i:usize)->&'static str{match i {0=>"success",1=>"rejected",_=>"failed"}}
fn series()->[AtomicU64;3]{array::from_fn(|_|AtomicU64::new(0))}
fn search_series()->[[AtomicU64;2];3]{array::from_fn(|_|array::from_fn(|_|AtomicU64::new(0)))}

pub struct Metrics {
    http_requests:[AtomicU64;3],
    http_duration_ms:AtomicU64,
    notion_requests:[AtomicU64;3],
    notion_duration_ms:AtomicU64,
    notion_retries:AtomicU64,
    notion_rate_limits:AtomicU64,
    search_requests:[[AtomicU64;2];3],
    search_duration_ms:[AtomicU64;3],
    webhook_admissions:[AtomicU64;3],
    queue_depth:AtomicI64,
    index_freshness_age_seconds:AtomicI64,
}
impl Default for Metrics {
    fn default()->Self{
        Self{
            http_requests:series(),http_duration_ms:AtomicU64::new(0),
            notion_requests:series(),notion_duration_ms:AtomicU64::new(0),
            notion_retries:AtomicU64::new(0),notion_rate_limits:AtomicU64::new(0),
            search_requests:search_series(),
            search_duration_ms:array::from_fn(|_|AtomicU64::new(0)),
            webhook_admissions:series(),
            queue_depth:AtomicI64::new(-1),
            index_freshness_age_seconds:AtomicI64::new(-1),
        }
    }
}

static GLOBAL:OnceLock<Metrics>=OnceLock::new();
pub fn global()->&'static Metrics{GLOBAL.get_or_init(Metrics::default)}

impl Metrics {
    pub fn observe(&self,operation:Operation,outcome:Outcome,elapsed:Duration){
        let i=outcome_index(outcome);
        let ms=u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        match operation {
            Operation::HttpRequest=>{
                self.http_requests[i].fetch_add(1,Ordering::Relaxed);
                self.http_duration_ms.fetch_add(ms,Ordering::Relaxed);
            }
            Operation::NotionApi=>{
                self.notion_requests[i].fetch_add(1,Ordering::Relaxed);
                self.notion_duration_ms.fetch_add(ms,Ordering::Relaxed);
            }
            Operation::WebhookAdmission=>{
                self.webhook_admissions[i].fetch_add(1,Ordering::Relaxed);
            }
            Operation::McpTool|Operation::IndexCommit=>{}
        }
    }
    pub fn notion_retry(&self){self.notion_retries.fetch_add(1,Ordering::Relaxed);}
    pub fn notion_rate_limit(&self){self.notion_rate_limits.fetch_add(1,Ordering::Relaxed);}
    pub fn observe_search(&self,mode:SearchMode,elapsed:Duration,success:bool){
        let i=mode.index();
        self.search_requests[i][usize::from(!success)].fetch_add(1,Ordering::Relaxed);
        self.search_duration_ms[i].fetch_add(
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),Ordering::Relaxed
        );
    }
    /// Snapshot from a **trusted** queue/index adapter. -1 explicitly means
    /// unavailable/unmeasured, never falsely claim zero backlog or fresh data.
    pub fn set_queue_depth(&self,count:Option<u64>){
        self.queue_depth.store(count.and_then(|x|i64::try_from(x).ok()).unwrap_or(-1),Ordering::Relaxed);
    }
    pub fn set_index_freshness_seconds(&self,age:Option<u64>){
        self.index_freshness_age_seconds.store(
            age.and_then(|x|i64::try_from(x).ok()).unwrap_or(-1),Ordering::Relaxed
        );
    }
    pub fn render(&self)->String {
        let mut out=String::with_capacity(2048);
        out.push_str("# HELP nk_http_requests_total HTTP request results\n# TYPE nk_http_requests_total counter\n");
        for i in 0..3 {
            let _=writeln!(&mut out,"nk_http_requests_total{{outcome=\"{}\"}} {}",label(i),
                self.http_requests[i].load(Ordering::Relaxed));
        }
        out.push_str("# TYPE nk_http_request_duration_seconds_summary summary\n");
        let total=(0..3).map(|i|self.http_requests[i].load(Ordering::Relaxed)).sum::<u64>();
        let _=writeln!(&mut out,"nk_http_request_duration_seconds_summary_sum {:.3}",
            self.http_duration_ms.load(Ordering::Relaxed) as f64/1000.0);
        let _=writeln!(&mut out,"nk_http_request_duration_seconds_summary_count {total}");
        out.push_str("# TYPE nk_notion_requests_total counter\n");
        for i in 0..3 {
            let _=writeln!(&mut out,"nk_notion_requests_total{{outcome=\"{}\"}} {}",label(i),
                self.notion_requests[i].load(Ordering::Relaxed));
        }
        out.push_str("# TYPE nk_notion_retry_total counter\n");
        let _=writeln!(&mut out,"nk_notion_retry_total {}",self.notion_retries.load(Ordering::Relaxed));
        out.push_str("# TYPE nk_notion_rate_limit_total counter\n");
        let _=writeln!(&mut out,"nk_notion_rate_limit_total {}",self.notion_rate_limits.load(Ordering::Relaxed));
        out.push_str("# TYPE nk_search_requests_total counter\n");
        for mode in [SearchMode::Lexical,SearchMode::Semantic,SearchMode::Hybrid] {
            let i=mode.index();
            for result in 0..2 {
                let _=writeln!(&mut out,
                    "nk_search_requests_total{{mode=\"{}\",outcome=\"{}\"}} {}",
                    mode.label(),if result==0 {"success"}else{"failed"},
                    self.search_requests[i][result].load(Ordering::Relaxed));
            }
            let _=writeln!(&mut out,"nk_search_duration_seconds_sum{{mode=\"{}\"}} {:.3}",
                mode.label(),self.search_duration_ms[i].load(Ordering::Relaxed) as f64/1000.0);
            let n=(0..2).map(|j|self.search_requests[i][j].load(Ordering::Relaxed)).sum::<u64>();
            let _=writeln!(&mut out,"nk_search_duration_seconds_count{{mode=\"{}\"}} {n}",mode.label());
        }
        out.push_str("# TYPE nk_webhook_admissions_total counter\n");
        for i in 0..3 {
            let _=writeln!(&mut out,"nk_webhook_admissions_total{{outcome=\"{}\"}} {}",label(i),
                self.webhook_admissions[i].load(Ordering::Relaxed));
        }
        out.push_str("# HELP nk_webhook_queue_depth -1 means queue backlog unavailable\n# TYPE nk_webhook_queue_depth gauge\n");
        let _=writeln!(&mut out,"nk_webhook_queue_depth {}",self.queue_depth.load(Ordering::Relaxed));
        out.push_str("# HELP nk_index_freshness_age_seconds -1 means index freshness unavailable\n# TYPE nk_index_freshness_age_seconds gauge\n");
        let _=writeln!(&mut out,"nk_index_freshness_age_seconds {}",self.index_freshness_age_seconds.load(Ordering::Relaxed));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_label_cardinality_and_true_unknown_gauges() {
        let m=Metrics::default();
        m.observe(Operation::HttpRequest,Outcome::Rejected,Duration::from_millis(25));
        m.observe(Operation::NotionApi,Outcome::Failed,Duration::from_millis(100));
        m.observe_search(SearchMode::Hybrid,Duration::from_millis(50),false);
        m.notion_retry();
        m.notion_rate_limit();
        let output=m.render();
        assert!(output.contains("nk_http_requests_total{outcome=\"rejected\"} 1"));
        assert!(output.contains("nk_notion_retry_total 1"));
        assert!(output.contains("nk_search_requests_total{mode=\"hybrid\",outcome=\"failed\"} 1"));
        assert!(output.contains("nk_webhook_queue_depth -1"));
        assert!(output.contains("nk_index_freshness_age_seconds -1"));
        assert!(!output.contains("page_id"));
        assert!(!output.contains("query="));
        m.set_queue_depth(Some(7));m.set_index_freshness_seconds(Some(42));
        let output=m.render();
        assert!(output.contains("nk_webhook_queue_depth 7"));
        assert!(output.contains("nk_index_freshness_age_seconds 42"));
    }
}
