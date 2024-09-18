use std::fs::OpenOptions;
use std::{collections::HashMap, io::Write};

use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE},
    Client,
};

use serde_json::Value;

async fn fetch_sparql_results(
    client: &Client,
    endpoint: &str,
    query: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut params = HashMap::new();
    params.insert("query", query);

    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/sparql-results+json"),
    );
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );

    let response = client
        .post(endpoint)
        .headers(headers)
        .form(&params)
        .send()
        .await?;

    let result: Value;

    if response.status().is_success() {
        let body = response.text().await?;
        result = serde_json::from_str(&body)?;
    } else {
        println!("Error: {:?}", response);
        println!("Status code: {:?}", response.status());
        result = serde_json::Value::Null;
    }

    Ok(result)
}

fn parse_json_uris(value: &serde_json::Value) -> Vec<&serde_json::Value> {
    let mut v: Vec<&serde_json::Value> = vec![];

    // Loop over the results and print them line by line
    if let Some(value) = value.get("results") {
        if let Some(bindings) = value.get("bindings") {
            if let Some(array) = bindings.as_array() {
                for binding in array {
                    // println!("{}", binding);
                    if binding["s"]["type"] == "uri" {
                        v.push(binding);
                    }
                }
            }
        }
    }

    v
}

fn build_delete_snippet(results: &Vec<&serde_json::Value>) -> String {
    let mut s = String::new();
    s.push_str(
        r#"DELETE {
  GRAPH ?g {
    ?s ?p ?o .
  }
}
WHERE {
  VALUES ?s {
"#,
    );

    let mut values = String::new();

    // Construct the VALUES snippet.
    for val in results {
        // println!("{}", val);
        values.push_str(&format!("    <{}>\n", &val["s"]["value"].as_str().unwrap()));
    }

    s.push_str(&values);
    s.push_str("  }\n");
    s.push_str(
        r#"
  GRAPH ?g {
    ?s ?p ?o .
  }
}
"#,
    );

    s
}

fn create_simple_forward_parametrized_delete_query(uri: &str) -> String {
    let query = format!(
        r#"DELETE {{
  GRAPH ?g {{
    ?s ?p ?o .
  }}
}}
WHERE {{
  BIND({} AS ?s)

  GRAPH ?g {{
    ?s ?p ?o .
  }}
}}"#,
        uri
    );

    query
}

fn create_forward_parametrized_query(uri: &str) -> String {
    let query = format!(
        r#"
      SELECT DISTINCT ?o WHERE {{
        VALUES ?values {{
          {}
        }}

        ?values ?p ?o .
      }}
    "#,
        uri
    );

    query
}

fn create_reverse_parametrized_query(uri: &str) -> String {
    // let query = format!(
    //     r#"
    //     SELECT DISTINCT ?s WHERE {{
    //       VALUES ?values {{
    //         {}
    //       }}

    //       ?s ?p ?values .
    //     }}
    // "#,
    //     uri.iter()
    //         .map(|&s| format!("<{}>", s))
    //         .collect::<Vec<_>>()
    //         .join("\n")
    // );

    // let query = format!(
    //     r#"
    //       SELECT DISTINCT ?s WHERE {{
    //         VALUES ?values {{
    //           {}
    //         }}

    //         ?s ?p ?values .
    //       }}
    //     "#,
    //     uri.iter()
    //         .filter_map(|v| { v["s"]["value"].as_str().map(String::from) })
    //         .collect::<Vec<_>>()
    //         .join("\n")
    // );

    let query = format!(
        r#"
        SELECT DISTINCT ?s WHERE {{
          VALUES ?values {{
            {}
          }}

          ?s ?p ?values .
        }}
    "#,
        uri
    );

    query
}

async fn build_reverse_path(uri: &str) -> Result<String, Box<dyn std::error::Error>> {
    const SPARQL_ENDPOINT: &str = "http://localhost:8890/sparql";
    let client = Client::new();

    let mut s = String::new();

    // Start with the initial URI and fetch all reverse subjects until nothing can be found.
    let get_initial_reverse_triples = create_reverse_parametrized_query(uri);

    let mut r = fetch_sparql_results(
        &client,
        SPARQL_ENDPOINT,
        get_initial_reverse_triples.as_str(),
    )
    .await?;

    let mut results = parse_json_uris(&r);

    while !results.is_empty() {
        s.push_str(build_delete_snippet(&results).as_str());
        s.push_str("\n;\n\n");

        // Construct URIs separated by new-lines.
        // These URIs will be used to create a parametrized query that fetches
        // reverse triples of these URIs.
        let uri_value_list = results
            .iter()
            .filter_map(|v| v["s"]["value"].as_str().map(|s| format!("<{}>", s)))
            // .map(|v| format!("<{}>", v["s"]["value"].as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let get_reverse_triples = create_reverse_parametrized_query(uri_value_list.as_str());
        r = fetch_sparql_results(&client, SPARQL_ENDPOINT, get_reverse_triples.as_str()).await?;
        results = parse_json_uris(&r);
    }

    Ok(s)
}

async fn build_forward_path(uri: &str) -> Result<String, Box<dyn std::error::Error>> {
    const SPARQL_ENDPOINT: &str = "http://localhost:8890/sparql";
    let client = Client::new();

    let mut s = String::new();

    // Start with the initial URI and fetch all reverse subjects until nothing can be found.
    let get_initial_forward_triples = create_forward_parametrized_query(uri);

    let mut r = fetch_sparql_results(
        &client,
        SPARQL_ENDPOINT,
        get_initial_forward_triples.as_str(),
    )
    .await?;

    let mut results = parse_json_uris(&r);

    while !results.is_empty() {
        s.push_str(build_delete_snippet(&results).as_str());
        s.push_str("\n;\n\n");

        // Construct URIs separated by new-lines.
        // These URIs will be used to create a parametrized query that fetches
        // reverse triples of these URIs.
        let uri_value_list = results
            .iter()
            .filter_map(|v| v["s"]["value"].as_str().map(|s| format!("<{}>", s)))
            // .map(|v| format!("<{}>", v["s"]["value"].as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        let get_forward_triples = create_forward_parametrized_query(uri_value_list.as_str());
        r = fetch_sparql_results(&client, SPARQL_ENDPOINT, get_forward_triples.as_str()).await?;
        results = parse_json_uris(&r);
    }

    Ok(s)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // const SPARQL_ENDPOINT: &str = "http://localhost:8870/sparql";
    const URI: &str =
        "<http://data.lblod.info/id/bestuurseenheden/cadbde55-c68c-47c1-8da1-905a171b844b>";

    // const GET_RESOURCES_QUERY: &str = r#"
    //   SELECT DISTINCT ?s WHERE {
    //     ?s ?p <http://data.lblod.info/id/bestuurseenheden/9af828073bb4c53989fe0693526a31aec47d85a4bc6ac9d485ca6878eb3b3f1c> .
    //   }
    // "#;

    // let GET_RESOURCES = format!(r#"
    //   SELECT DISTINCT ?s WHERE {{
    //     ?s ?p {} .
    //   }}
    // "#, "<http://data.lblod.info/id/bestuurseenheden/9af828073bb4c53989fe0693526a31aec47d85a4bc6ac9d485ca6878eb3b3f1c>");

    // let client = Client::new();

    // let get_resources_query = create_parametrized_query(&vec!["http://data.lblod.info/id/bestuurseenheden/9af828073bb4c53989fe0693526a31aec47d85a4bc6ac9d485ca6878eb3b3f1c"]);

    // let r = fetch_sparql_results(&client, SPARQL_ENDPOINT, get_resources_query.as_str()).await?;
    // let results = parse_json_uris(&r);
    // println!("{:?}", results);
    // let out = build_delete_snippet(&results);
    let out = build_reverse_path(URI).await?;
    println!("{}", out);

    let out_forward = build_forward_path(URI).await?;
    println!("{}", out_forward);

    // let mut file = OpenOptions::new()
    //     .create(true)
    //     .append(true)
    //     .open(format!("{}/{}", "out_folder", "output.json"))?;

    // let json_string = serde_json::to_string_pretty(&results)?;
    // file.write_all(json_string.as_bytes())?;

    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}/{}", "out_folder", "output.txt"))?;
    // f.write_all("<uri1> a ?type".as_bytes())?;
    f.write_all("# Delete reverse triples\n\n".as_bytes())?;
    f.write_all(out.as_bytes())?;

    f.write_all("# Delete forward triples\n\n".as_bytes())?;
    // f.write_all(out_forward.as_bytes())?;
    f.write_all(create_simple_forward_parametrized_delete_query(URI).as_bytes())?;

    Ok(())
}
