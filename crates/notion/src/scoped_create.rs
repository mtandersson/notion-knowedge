//! Server-only, scope-constrained Notion page create with canonical readback.
//! A successful POST receipt is never proof of a visible authorized page.
use std::{sync::Arc, time::Duration};
use crate::{NotionClient, pages, writes::MARKDOWN_VERSION};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageId},
    lifecycle::LifecycleScope,
    root_scope::{RootScopeGate, ScopeError},
};
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};

const CREATE: &str = "notion.page.scoped_create";
const VERIFY: &str = "notion.page.scoped_create.verify";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedPageCreated {
    pub page_id: PageId,
    pub parent_page_id: PageId,
    pub url: String,
    pub verified: bool,
}

#[derive(Debug, Clone)]
pub struct ScopedPageCreateInput {
    pub parent_page_id: PageId,
    pub title: String,
    pub markdown: String,
    /// Optional Notion emoji icon: never accepts arbitrary properties JSON.
    pub icon_emoji: Option<String>,
}

fn err(op: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {operation:op,kind,retry_after:None,committed_page_id:None}
}
fn scope_error(e: ScopeError) -> BackendError {
    err(CREATE,match e {
        ScopeError::Denied => BackendErrorKind::PermissionDenied,
        ScopeError::Changed => BackendErrorKind::Conflict,
        ScopeError::Unverifiable => BackendErrorKind::Unavailable,
        ScopeError::InvalidConfiguration => BackendErrorKind::InvalidInput,
    })
}
fn safe_text(s:&str) -> bool {
    !s.chars().any(|ch| ch.is_control() || matches!(
        ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
    ))
}
fn valid(req: &ScopedPageCreateInput) -> bool {
    pages::uuid(&req.parent_page_id.0).as_deref()==Some(req.parent_page_id.0.as_str())
        && !req.title.trim().is_empty()
        && req.title.chars().count()<=200
        && safe_text(&req.title)
        && req.markdown.len()<=250*1024
        && safe_text(&req.markdown)
        && req.icon_emoji.as_deref().is_none_or(|s|
            !s.is_empty() && s.chars().count()<=8 && safe_text(s) && !s.chars().any(char::is_whitespace))
}
fn created_id(receipt:&Value)->Option<(PageId,String)> {
    if receipt["object"]!="page" || receipt["in_trash"]==true || receipt["archived"]==true {
        return None;
    }
    let id=pages::uuid(receipt["id"].as_str()?)?;
    let url=receipt["url"].as_str()?;
    if !url.starts_with("https://") || url.len()>4096 {return None;}
    Some((PageId(id),url.into()))
}
fn verified_page(value:&Value,id:&PageId,parent:&PageId,url:&str,title:&str)->bool {
    if created_id(value).is_none_or(|(got,read_url)| got!=*id || read_url!=url){
        return false;
    }
    if value["parent"]["type"]!="page_id" ||
        value["parent"]["page_id"].as_str().and_then(pages::uuid).as_deref()!=Some(parent.0.as_str()) {
        return false;
    }
    value["properties"]["title"]["title"].as_array()
        .is_some_and(|items| items.len()==1 && items[0]["plain_text"].as_str()==Some(title))
}

impl NotionClient {
    /// Only trusted server-owned `scope` may authorize the explicit parent.
    /// The physical parent ancestry is fetched and revalidated before POST.
    /// No uncertain POST is replayed, even if readback never materializes.
    pub async fn create_scoped_page(
        &self,
        request: &ScopedPageCreateInput,
        scope: &LifecycleScope,
    ) -> Result<ScopedPageCreated,BackendError> {
        if !valid(request){return Err(err(CREATE,BackendErrorKind::InvalidInput));}
        let gate=RootScopeGate::new(Arc::new(self.clone()),scope.clone()).map_err(scope_error)?;
        let permit=gate.authorize(&request.parent_page_id).await.map_err(scope_error)?;
        gate.revalidate(&permit,scope).await.map_err(scope_error)?;
        let mut body=json!({
            "parent":{"type":"page_id","page_id":request.parent_page_id.0},
            "properties":{"title":{"type":"title","title":[{"type":"text","text":{"content":request.title}}]}},
            "markdown":request.markdown
        });
        if let Some(emoji)=&request.icon_emoji {
            body["icon"]=json!({"type":"emoji","emoji":emoji});
        }
        let receipt=self.write(reqwest::Method::POST,"/pages",body,CREATE).await?;
        let (id,url)=created_id(&receipt).ok_or_else(||err(CREATE,BackendErrorKind::Conflict))?;
        // Strongly check the receipt's parent when available, then read the
        // authoritative created page. The receipt cannot stand in for a GET.
        if receipt["parent"]["type"]!="page_id" ||
            receipt["parent"]["page_id"].as_str().and_then(pages::uuid).as_deref()
                !=Some(request.parent_page_id.0.as_str()) {
            return Err(err(CREATE,BackendErrorKind::Conflict));
        }
        for attempt in 0..3 {
            let http=self.http.get(format!("{}/pages/{}",self.api_root,id.0))
                .header(AUTHORIZATION,self.authorization.clone())
                .header("Notion-Version",MARKDOWN_VERSION);
            let mut response=self.send(http,true,VERIFY).await?;
            match response.status().as_u16() {
                404 => {},
                200 => {
                    let mut data=Vec::new();
                    while let Some(chunk)=response.chunk().await.map_err(|_|err(VERIFY,BackendErrorKind::Unavailable))? {
                        if data.len().saturating_add(chunk.len())>64*1024 {
                            return Err(err(VERIFY,BackendErrorKind::Internal));
                        }
                        data.extend_from_slice(&chunk);
                    }
                    let page:Value=serde_json::from_slice(&data)
                        .map_err(|_|err(VERIFY,BackendErrorKind::Internal))?;
                    if verified_page(&page,&id,&request.parent_page_id,&url,&request.title) {
                        return Ok(ScopedPageCreated{
                            page_id:id,parent_page_id:request.parent_page_id.clone(),url,verified:true
                        });
                    }
                },
                401 => return Err(err(VERIFY,BackendErrorKind::Unauthenticated)),
                403 => return Err(err(VERIFY,BackendErrorKind::PermissionDenied)),
                429 => return Err(err(VERIFY,BackendErrorKind::RateLimited)),
                500..=599 => return Err(err(VERIFY,BackendErrorKind::Unavailable)),
                _ => return Err(err(VERIFY,BackendErrorKind::Conflict)),
            }
            if attempt<2 {tokio::time::sleep(Duration::from_millis(150*(attempt+1))).await;}
        }
        Err(err(VERIFY,BackendErrorKind::Conflict))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT:&str="11111111-1111-1111-1111-111111111111";
    const CHILD:&str="22222222-2222-2222-2222-222222222222";
    fn input()->ScopedPageCreateInput {
        ScopedPageCreateInput{
            parent_page_id:PageId(PARENT.into()),
            title:"A new page".into(),markdown:"Body".into(),icon_emoji:Some("📄".into())
        }
    }
    fn receipt()->Value {
        json!({"object":"page","id":CHILD,"url":format!("https://www.notion.so/{CHILD}"),
            "parent":{"type":"page_id","page_id":PARENT},"in_trash":false})
    }
    #[test]
    fn input_rejects_bad_parent_content_and_icons() {
        let mut q=input();
        assert!(valid(&q));
        q.parent_page_id=PageId("../escape".into());
        assert!(!valid(&q));
        q=input();q.title="".into();assert!(!valid(&q));
        q=input();q.icon_emoji=Some("secret\n".into());assert!(!valid(&q));
        q=input();q.markdown="unsafe\u{202e}suffix".into();assert!(!valid(&q));
        q=input();q.markdown="X".repeat(250*1024+1);assert!(!valid(&q));
    }

    #[test]
    fn readback_rejects_wrong_parent_title_url_and_deleted_status() {
        let mut p=receipt();
        p["properties"]=json!({"title":{"title":[{"plain_text":"A new page"}]}});
        assert!(verified_page(&p,&PageId(CHILD.into()),&PageId(PARENT.into()),
            &format!("https://www.notion.so/{CHILD}"),"A new page"));
        p["parent"]["page_id"]=json!(CHILD);
        assert!(!verified_page(&p,&PageId(CHILD.into()),&PageId(PARENT.into()),
            &format!("https://www.notion.so/{CHILD}"),"A new page"));
        p["parent"]["page_id"]=json!(PARENT);
        p["in_trash"]=json!(true);
        assert!(!verified_page(&p,&PageId(CHILD.into()),&PageId(PARENT.into()),
            &format!("https://www.notion.so/{CHILD}"),"A new page"));
    }
}
