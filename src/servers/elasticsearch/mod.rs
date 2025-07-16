// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod base_tools;

use crate::servers::IncludeExclude;
use crate::utils::none_if_empty_string;
use elasticsearch::Elasticsearch;
use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::http::Url;
use elasticsearch::http::response::Response;
use http::header::USER_AGENT;
use http::request::Parts;
use http::{HeaderValue, header};
use indexmap::IndexMap;
use rmcp::RoleServer;
use rmcp::model::ToolAnnotations;
use rmcp::service::RequestContext;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_aux::field_attributes::deserialize_bool_from_anything;
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchMcpConfig {
    /// Cluster URL (can be comma-separated for multiple URLs)
    pub url: String,

    /// API key
    #[serde(default, deserialize_with = "none_if_empty_string")]
    pub api_key: Option<String>,

    /// Login
    #[serde(default, deserialize_with = "none_if_empty_string")]
    pub login: Option<String>,

    /// Password
    #[serde(default, deserialize_with = "none_if_empty_string")]
    pub password: Option<String>,

    /// Should we skip SSL certificate verification?
    #[serde(default, deserialize_with = "deserialize_bool_from_anything")]
    pub ssl_skip_verify: bool,

    #[serde(default, deserialize_with = "none_if_empty_string")]
    pub ca_cert: Option<String>,

    /// Search templates to expose as tools or resources
    #[serde(default)]
    pub tools: Tools,

    /// Prompts
    #[serde(default)]
    pub prompts: Vec<String>,
    // TODO: search as resources?
}

// A wrapper around an ES client that provides a client instance configured
/// for a given request context (i.e. auth credentials)
#[derive(Clone)]
pub struct EsClientProvider(Elasticsearch);

impl EsClientProvider {
    pub fn new(client: Elasticsearch) -> Self {
        EsClientProvider(client)
    }

    /// If the incoming request is a http request and has an `Authorization` header, use it
    /// to authenticate to the remote ES instance.
    pub fn get(&self, context: RequestContext<RoleServer>) -> Cow<Elasticsearch> {
        let client = &self.0;

        let Some(mut auth) = context
            .extensions
            .get::<Parts>()
            .and_then(|p| p.headers.get(header::AUTHORIZATION))
            .and_then(|h| h.to_str().ok())
        else {
            // No auth
            return Cow::Borrowed(client);
        };

        // MCP inspector insists on sending a bearer token and prepends "Bearer" to the value provided
        if auth.starts_with("Bearer ApiKey ") || auth.starts_with("Bearer Basic ") {
            auth = auth.trim_start_matches("Bearer ");
        }

        let transport = client
            .transport()
            .clone_with_auth(Some(Credentials::AuthorizationHeader(auth.to_string())));

        Cow::Owned(Elasticsearch::new(transport))
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Tools {
    #[serde(flatten)]
    pub incl_excl: Option<IncludeExclude>,
    pub custom: HashMap<String, CustomTool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CustomTool {
    Esql(EsqlTool),
    SearchTemplate(SearchTemplateTool),
}

impl CustomTool {
    pub fn base(&self) -> &ToolBase {
        match self {
            CustomTool::Esql(esql) => &esql.base,
            CustomTool::SearchTemplate(search_template) => &search_template.base,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolBase {
    pub description: String,
    pub parameters: IndexMap<String, schemars::schema::SchemaObject>,
    pub annotations: Option<ToolAnnotations>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EsqlTool {
    #[serde(flatten)]
    base: ToolBase,
    query: String,
    #[serde(default)]
    format: EsqlResultFormat,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EsqlResultFormat {
    #[default]
    // Output as JSON, either as an array of objects or as a single object.
    Json,
    // If a single object with a single property, output only its value
    Value,
    //Csv,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchTemplateTool {
    #[serde(flatten)]
    base: ToolBase,
    #[serde(flatten)]
    template: SearchTemplate,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchTemplate {
    TemplateId(String),
    Template(serde_json::Value), // or constrain to an object?
}

#[derive(Clone)]
pub struct ElasticsearchMcp {}

impl ElasticsearchMcp {
    pub async fn new_with_config(config: ElasticsearchMcpConfig) -> anyhow::Result<base_tools::EsBaseTools> {
        let creds = if let Some(api_key) = config.api_key.clone() {
            Some(Credentials::EncodedApiKey(api_key))
        } else if let Some(login) = config.login.clone() {
            let pwd = config.password.clone().ok_or(anyhow::Error::msg("missing password"))?;
            Some(Credentials::Basic(login, pwd))
        } else {
            None
        };

        let url_str = config.url.as_str();
        if url_str.is_empty() {
            return Err(anyhow::Error::msg("Elasticsearch URL is empty"));
        }

        let urls: Result<Vec<Url>, _> = url_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| Url::parse(s))
            .collect();
        let urls = urls?;

        if urls.is_empty() {
            return Err(anyhow::Error::msg("No valid Elasticsearch URLs provided"));
        }

        let mut transport = if urls.len() == 1 {
            let pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(urls[0].clone());
            elasticsearch::http::transport::TransportBuilder::new(pool)
        } else {
            let pool = elasticsearch::http::transport::MultiNodeConnectionPool::round_robin(urls, None);
            elasticsearch::http::transport::TransportBuilder::new(pool)
        };
        
        if let Some(creds) = creds {
            transport = transport.auth(creds);
        }
        
        if config.ssl_skip_verify {
            transport = transport.cert_validation(CertificateValidation::None);
        } else if let Some(ca_cert_pem) = &config.ca_cert {
            let cert = elasticsearch::cert::Certificate::from_pem(ca_cert_pem.as_bytes())?;
            transport = transport.cert_validation(CertificateValidation::Full(cert));
        }
        
        transport = transport.header(
            USER_AGENT,
            HeaderValue::from_str(&format!("elastic-mcp/{}", env!("CARGO_PKG_VERSION")))?,
        );
        
        let transport = transport.build()?;
        let es_client = Elasticsearch::new(transport);

        let version = Self::detect_version(&es_client).await.unwrap_or((8, 0));
        
        Ok(base_tools::EsBaseTools::new_with_version(es_client, version))
    }

    async fn detect_version(client: &Elasticsearch) -> anyhow::Result<(u32, u32)> {
        let response = client
            .send(
                elasticsearch::http::Method::Get,
                "/_info/_all",
                elasticsearch::http::headers::HeaderMap::new(),
                Option::<&()>::None,
                None::<()>,
                None,
            )
            .await?;
        
        let cluster_info: ClusterInfoResponse = response.json().await?;
        parse_version(&cluster_info.version.number)
            .ok_or_else(|| anyhow::Error::msg("Failed to parse Elasticsearch version"))
    }
}

//------------------------------------------------------------------------------------------------
// Utilities

/// Map any error to an internal error of the MCP server
pub fn internal_error(e: impl std::error::Error) -> rmcp::Error {
    rmcp::Error::internal_error(e.to_string(), None)
}

/// Return an error as an error response to the client, which may be able to take
/// action to correct it. This should be refined to handle common error types such
/// as index not found, which could be caused by the client hallucinating an index name.
///
/// TODO (in rmcp): if rmcp::Error had a variant that accepts a CallToolResult, this would
/// allow to use the '?' operator while sending a result to the client.
pub fn handle_error(result: Result<Response, elasticsearch::Error>) -> Result<Response, rmcp::Error> {
    match result {
        Ok(resp) => resp.error_for_status_code(),
        Err(e) => {
            tracing::error!("Error: {:?}", &e);
            Err(e)
        }
    }
    .map_err(internal_error)
}

pub async fn read_json<T: DeserializeOwned>(
    response: Result<Response, elasticsearch::Error>,
) -> Result<T, rmcp::Error> {
    // let text = read_text(response).await?;
    // tracing::debug!("Received json {text}");
    // serde_json::from_str(&text).map_err(internal_error)

    let response = handle_error(response)?;
    response.json().await.map_err(internal_error)
}

#[allow(dead_code)]
pub async fn read_text(result: Result<Response, elasticsearch::Error>) -> Result<String, rmcp::Error> {
    let response = handle_error(result)?;
    response.text().await.map_err(internal_error)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterInfoResponse {
    pub cluster_name: String,
    pub version: VersionInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub number: String,
    pub build_flavor: String,
    pub build_type: String,
    pub build_hash: String,
    pub build_date: String,
    pub build_snapshot: bool,
    pub lucene_version: String,
    pub minimum_wire_compatibility_version: String,
    pub minimum_index_compatibility_version: String,
}

pub fn parse_version(version_str: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = version_str.split('.').collect();
    if parts.len() >= 2 {
        if let (Ok(major), Ok(minor)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
            return Some((major, minor));
        }
    }
    None
}
