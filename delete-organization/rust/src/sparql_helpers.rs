use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE},
    Client,
};

pub struct SparqlClient {
    client: Client,
    endpoint: String,
}

impl SparqlClient {
    pub fn new(endpoint: String) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/sparql-results+json"),
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );

        let client = Client::builder().default_headers(headers).build().unwrap();

        SparqlClient { client, endpoint }
    }

    pub async fn query(&self, query: &str) -> Result<serde_json::Value, reqwest::Error> {
        let params = [("query", query)];
        let response = self.client
            .post(&self.endpoint)
            .form(&params)
            .send()
            .await?
            .json()
            .await?;

        Ok(response)
    }
}
