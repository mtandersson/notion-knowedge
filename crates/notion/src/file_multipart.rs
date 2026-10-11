//! Opt-in Notion multipart upload for operator-approved large files.
//!
//! Failed/ambiguous parts are never silently replayed, and no attachment or
//! public MCP file upload becomes authorized merely because the upload exists.
use crate::{
    NotionClient,
    file_download::DownloadedFile,
    file_upload::{UploadStatus, UploadedFile},
    file_validation::FileValidationPolicy,
    pages,
    writes::MARKDOWN_VERSION,
};
use notion_knowledge_core::backend::{BackendError,BackendErrorKind};
use reqwest::{header::AUTHORIZATION, multipart::{Form,Part}};
use serde_json::{Value,json};
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt,AsyncSeekExt};
use tokio_util::io::ReaderStream;

const CREATE:&str="notion.multipart.create";
const SEND:&str="notion.multipart.send";
const COMPLETE:&str="notion.multipart.complete";
const VERIFY:&str="notion.multipart.verify";
const MIB:usize=1024*1024;
const MAX_BYTES:usize=100*MIB;
const MAX_PARTS:usize=1000;
const RECEIPT_BYTES:usize=64*1024;

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct MultipartPolicy {
    pub maximum_bytes:usize,
    pub threshold_bytes:usize,
    pub part_bytes:usize,
}

impl MultipartPolicy {
    /// Caller/operator must opt in to a file plan permitting >20MiB uploads.
    /// Keep the maximum within this adapter's 100 MiB cap; default is OFF.
    pub fn new(maximum_bytes:usize,threshold_bytes:usize,part_bytes:usize)->Result<Self,BackendError>{
        if threshold_bytes<20*MIB || maximum_bytes<=threshold_bytes ||
            maximum_bytes>MAX_BYTES || !(5*MIB..=20*MIB).contains(&part_bytes){
            return Err(err(CREATE,BackendErrorKind::InvalidInput));
        }
        Ok(Self{maximum_bytes,threshold_bytes,part_bytes})
    }
    pub fn parts(self,bytes:usize)->Result<usize,BackendError>{
        if bytes<=self.threshold_bytes || bytes>self.maximum_bytes {
            return Err(err(CREATE,BackendErrorKind::InvalidInput));
        }
        let n=bytes.div_ceil(self.part_bytes);
        if n<2 || n>MAX_PARTS {return Err(err(CREATE,BackendErrorKind::InvalidInput));}
        Ok(n)
    }
}

fn err(op:&'static str,kind:BackendErrorKind)->BackendError {
    BackendError{operation:op,kind,retry_after:None,committed_page_id:None}
}

fn check_upload(value:&Value,id:&str,status:&str,parts:usize)->bool {
    value["object"]=="file_upload"
        && value["status"]==status
        && value["in_trash"]!=true
        && value["id"].as_str().and_then(pages::uuid).as_deref()==Some(id)
        && value["number_of_parts"]["total"].as_u64()==Some(parts as u64)
}
fn status_kind(status:u16)->BackendErrorKind {
    match status {
        400|413|415=>BackendErrorKind::InvalidInput,
        401=>BackendErrorKind::Unauthenticated,
        403=>BackendErrorKind::PermissionDenied,
        404=>BackendErrorKind::NotFound,
        409=>BackendErrorKind::Conflict,
        429|529=>BackendErrorKind::RateLimited,
        500..=599=>BackendErrorKind::Unavailable,
        _=>BackendErrorKind::Internal
    }
}
async fn receipt(mut response:reqwest::Response,op:&'static str)->Result<Value,BackendError>{
    if response.status().as_u16()!=200 {
        return Err(err(op,status_kind(response.status().as_u16())));
    }
    let mut bytes=Vec::new();
    while let Some(chunk)=response.chunk().await.map_err(|_|err(op,BackendErrorKind::Unavailable))? {
        if bytes.len().saturating_add(chunk.len())>RECEIPT_BYTES {
            return Err(err(op,BackendErrorKind::Internal));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_|err(op,BackendErrorKind::Internal))
}

impl NotionClient {
    /// Multipart transfer of a request-owned downloaded temp file, never a URL.
    /// Validation is performed before creating any upload session. Each part
    /// uses a freshly reopened independent cursor and a single non-replayed send.
    /// Completion is followed by a separate authoritative status GET.
    pub async fn upload_multipart(
        &self,
        file:&DownloadedFile,
        filename:&str,
        declared_mime:&str,
        policy:MultipartPolicy
    )->Result<UploadedFile,BackendError>{
        let size=file.size_bytes();
        let parts=policy.parts(size)?;
        // Large binary signatures can be validated from a bounded prefix.
        // Text/CSV/Markdown are intentionally excluded: validating their entire
        // UTF-8 content would need a separate streaming content validator.
        if !matches!(declared_mime,"application/pdf"|"image/png"|"image/jpeg") {
            return Err(err(CREATE,BackendErrorKind::InvalidInput));
        }
        let mut handle=tokio::fs::File::from_std(file.reopen().map_err(|_|err(CREATE,BackendErrorKind::Unavailable))?);
        let mut prefix=vec![0u8;64*1024];
        let len=handle.read(&mut prefix).await.map_err(|_|err(CREATE,BackendErrorKind::Unavailable))?;
        prefix.truncate(len);
        let valid=FileValidationPolicy::for_single_part(64*1024)
            .map_err(|_|err(CREATE,BackendErrorKind::InvalidInput))?
            .validate(filename,declared_mime,&prefix)
            .map_err(|_|err(CREATE,BackendErrorKind::InvalidInput))?;
        drop(handle);

        let created=self.write(reqwest::Method::POST,"/file_uploads",json!({
            "mode":"multi_part","filename":valid.filename,
            "content_type":valid.mime_type,"number_of_parts":parts
        }),CREATE).await?;
        let id=created["id"].as_str().and_then(pages::uuid)
            .ok_or_else(||err(CREATE,BackendErrorKind::Conflict))?;
        if !check_upload(&created,&id,"pending",parts) {
            return Err(err(CREATE,BackendErrorKind::Conflict));
        }

        for index in 0..parts {
            let offset=index*policy.part_bytes;
            let count=(size-offset).min(policy.part_bytes);
            let mut source=tokio::fs::File::from_std(file.reopen()
                .map_err(|_|err(SEND,BackendErrorKind::Unavailable))?);
            source.seek(SeekFrom::Start(offset as u64)).await
                .map_err(|_|err(SEND,BackendErrorKind::Unavailable))?;
            let bounded=source.take(count as u64);
            let part=Part::stream_with_length(
                reqwest::Body::wrap_stream(ReaderStream::new(bounded)),count as u64
            ).file_name(valid.filename.clone()).mime_str(valid.mime_type)
                .map_err(|_|err(SEND,BackendErrorKind::InvalidInput))?;
            let request=self.http.post(format!("{}/file_uploads/{id}/send",self.api_root))
                .header(AUTHORIZATION,self.authorization.clone())
                .header("Notion-Version",MARKDOWN_VERSION)
                .multipart(Form::new().part("file",part).text("part_number",(index+1).to_string()));
            let response=self.send(request,false,SEND).await?;
            let sent=receipt(response,SEND).await?;
            if !check_upload(&sent,&id,"pending",parts) ||
                sent["number_of_parts"]["sent"].as_u64().is_none_or(|count|count<=(index as u64)){
                return Err(err(SEND,BackendErrorKind::Conflict));
            }
        }

        let completed=self.write(reqwest::Method::POST,
            &format!("/file_uploads/{id}/complete"),json!({}),COMPLETE).await?;
        if !check_upload(&completed,&id,"uploaded",parts) {
            return Err(err(COMPLETE,BackendErrorKind::Conflict));
        }
        let request=self.http.get(format!("{}/file_uploads/{id}",self.api_root))
            .header(AUTHORIZATION,self.authorization.clone())
            .header("Notion-Version",MARKDOWN_VERSION);
        let returned=receipt(self.send(request,true,VERIFY).await?,VERIFY).await?;
        if !check_upload(&returned,&id,"uploaded",parts) {
            return Err(err(VERIFY,BackendErrorKind::Conflict));
        }
        Ok(UploadedFile{
            id,status:UploadStatus::Uploaded,filename:valid.filename,
            mime_type:valid.mime_type.into(),size_bytes:size
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const ID:&str="11111111-1111-1111-1111-111111111111";

    #[test]
    fn plan_enforces_opt_in_threshold_part_count_and_plan_cap(){
        assert!(MultipartPolicy::new(5*MIB,20*MIB,5*MIB).is_err());
        assert!(MultipartPolicy::new(101*MIB,20*MIB,5*MIB).is_err());
        assert!(MultipartPolicy::new(100*MIB,20*MIB,4*MIB).is_err());
        let p=MultipartPolicy::new(80*MIB,20*MIB,10*MIB).unwrap();
        assert_eq!(p.parts(25*MIB),Ok(3));
        assert!(p.parts(10*MIB).is_err());
        assert!(p.parts(81*MIB).is_err());
    }
    #[test]
    fn rejects_bogus_or_interrupted_upload_receipts(){
        let ok=json!({"object":"file_upload","id":ID,"status":"uploaded",
            "number_of_parts":{"total":3,"sent":3}});
        assert!(check_upload(&ok,ID,"uploaded",3));
        for change in [
            json!({"object":"file_upload","id":ID,"status":"pending","number_of_parts":{"total":3}}),
            json!({"object":"file_upload","id":ID,"status":"uploaded","number_of_parts":{"total":2}}),
            json!({"object":"file_upload","id":"22222222-2222-2222-2222-222222222222",
                "status":"uploaded","number_of_parts":{"total":3}}),
        ]{
            assert!(!check_upload(&change,ID,"uploaded",3));
        }
    }
}
