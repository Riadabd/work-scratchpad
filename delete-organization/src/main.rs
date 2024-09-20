use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::{collections::HashMap, io::Write};

use indexmap::IndexMap;
use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE},
    Client,
};

use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct jsonConfig {
    #[serde(flatten)]
    data: IndexMap<String, serde_json::Value>,
}

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

fn parse_json_uris<'a>(
    value: &'a serde_json::Value,
    target: &'a str,
) -> Vec<&'a serde_json::Value> {
    let mut v: Vec<&serde_json::Value> = vec![];

    // Loop over the results and print them line by line
    if let Some(value) = value.get("results") {
        if let Some(bindings) = value.get("bindings") {
            if let Some(array) = bindings.as_array() {
                for binding in array {
                    // println!("{}", binding);
                    if binding[target]["type"] == "uri" {
                        v.push(binding);
                    }
                }
            }
        }
    }

    v
}

fn build_delete_snippet(results: &Vec<&serde_json::Value>, target: &str) -> String {
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
        values.push_str(&format!(
            "    <{}>\n",
            &val[target]["value"].as_str().unwrap()
        ));
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

fn create_forward_parametrized_select_query_with_type(uri: &str, uri_type: &str) -> String {
    let query = format!(
        r#"
    SELECT DISTINCT ?o WHERE {{
      VALUES ?values {{
        {}
      }}

      ?values ?p ?o .
      ?o a {} .
    }}
  "#,
        uri, uri_type
    );

    query
}

fn create_backward_parametrized_select_query_with_type(uri: &str, uri_type: &str) -> String {
    let query = format!(
        r#"
    SELECT DISTINCT ?s WHERE {{
      VALUES ?values {{
        {}
      }}

      ?s a {} ;
        ?p ?values .
    }}
  "#,
        uri, uri_type
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
    const SPARQL_ENDPOINT: &str = "http://localhost:8870/sparql";
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

    let mut results = parse_json_uris(&r, "s");

    while !results.is_empty() {
        s.push_str(build_delete_snippet(&results, "s").as_str());
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
        results = parse_json_uris(&r, "s");
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

    let mut results = parse_json_uris(&r, "s");

    while !results.is_empty() {
        s.push_str(build_delete_snippet(&results, "s").as_str());
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
        results = parse_json_uris(&r, "s");
    }

    Ok(s)
}

async fn build_deletion_path(
    uri: &str,
    uri_type: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open("config/config-op.json")?;
    let reader = BufReader::new(file);
    // let my_data: Value = serde_json::from_reader(reader)?;
    let parsed_json_config: jsonConfig = serde_json::from_reader(reader)?;

    let mut map: HashMap<&str, Vec<String>> = HashMap::new();

    const SPARQL_ENDPOINT: &str = "http://localhost:8870/sparql";
    let client = Client::new();

    let mut s = String::new();

    map.insert(uri_type, vec![uri.to_string()]);

    // if let Some(obj) = parsed_json_config.as_object() {
        for (key, value) in &parsed_json_config.data {
            println!("{}", key);
            if let Some(inner_obj) = value.as_object() {
                if let Some(reverse) = inner_obj.get("reverse") {
                    if let Some(reverse_array) = reverse.as_array() {
                        for item in reverse_array {
                            // Fetch URIs belonging to the current key (type).
                            // These URIs were placed in the hashmap in a previous step
                            // where their type was in the reverse/forward array of a previous type.
                            // We fetch them to get their reverse triples.
                            if let Some(current_uris) = map.get(key.as_str()) {
                                let values_list = current_uris
                                    .iter()
                                    .map(|v| format!("{}", v))
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                // println!("{}", values_list);
                                let get_reverse_triples =
                                    create_backward_parametrized_select_query_with_type(
                                        values_list.as_str(),
                                        item.as_str().unwrap(),
                                    );
                                // println!("{}", get_reverse_triples);
                                let r = fetch_sparql_results(
                                    &client,
                                    SPARQL_ENDPOINT,
                                    get_reverse_triples.as_str(),
                                )
                                .await?;

                                let results = parse_json_uris(&r, "s");
                                let result_value_list = results
                                    .iter()
                                    .filter_map(|v| {
                                        v["s"]["value"].as_str().map(|s| format!("<{}>", s))
                                    })
                                    .collect::<Vec<_>>();
                                if !result_value_list.is_empty() {
                                    map.insert(item.as_str().unwrap(), result_value_list);

                                    s.push_str(build_delete_snippet(&results, "s").as_str());
                                    s.push_str("\n;\n\n");
                                }
                            }
                        }
                    }
                }

                if let Some(forward) = inner_obj.get("forward") {
                    if let Some(forward_array) = forward.as_array() {
                        for item in forward_array {
                            // Fetch URIs belonging to the current key (type).
                            // These URIs were placed in the hashmap in a previous step
                            // where their type was in the reverse/forward array of a previous type.
                            // We fetch them to get their forward triples.
                            if let Some(current_uris) = map.get(key.as_str()) {
                                let values_list = current_uris
                                    .iter()
                                    .map(|v| format!("{}", v))
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                // println!("{}", values_list);
                                let get_forward_triples =
                                    create_forward_parametrized_select_query_with_type(
                                        values_list.as_str(),
                                        item.as_str().unwrap(),
                                    );
                                // println!("{}", get_forward_triples);
                                let r = fetch_sparql_results(
                                    &client,
                                    SPARQL_ENDPOINT,
                                    get_forward_triples.as_str(),
                                )
                                .await?;

                                let results = parse_json_uris(&r, "o");
                                // println!("{:?}", results);
                                let result_value_list = results
                                    .iter()
                                    .filter_map(|v| {
                                        v["o"]["value"].as_str().map(|s| format!("<{}>", s))
                                    })
                                    .collect::<Vec<_>>();
                                if !result_value_list.is_empty() {
                                    map.insert(item.as_str().unwrap(), result_value_list);

                                    s.push_str(build_delete_snippet(&results, "o").as_str());
                                    s.push_str("\n;\n\n");
                                }
                            }
                        }
                    }
                }
            }
        }
    // }

    Ok(s)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // const SPARQL_ENDPOINT: &str = "http://localhost:8870/sparql";
    const URI: &str =
        "<http://data.lblod.info/id/bestuurseenheden/9af828073bb4c53989fe0693526a31aec47d85a4bc6ac9d485ca6878eb3b3f1c>";
    const URI_TYPE: &str = "<http://data.vlaanderen.be/ns/besluit#Bestuurseenheid>";

    // let out = build_reverse_path(URI).await?;
    // println!("{}", out);
    let out = build_deletion_path(URI, URI_TYPE).await?;
    // println!("{}", out);

    //let out_forward = build_forward_path(URI).await?;
    // println!("{}", out_forward);

    // let mut file = OpenOptions::new()
    //     .create(true)
    //     .append(true)
    //     .open(format!("{}/{}", "out_folder", "output.json"))?;

    // let json_string = serde_json::to_string_pretty(&results)?;
    // file.write_all(json_string.as_bytes())?;

    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}/{}", "generated_sparql_queries", "output.txt"))?;
    // f.write_all("<uri1> a ?type".as_bytes())?;
    f.write_all("# Delete reverse triples\n\n".as_bytes())?;
    f.write_all(out.as_bytes())?;

    f.write_all("# Delete forward triples\n\n".as_bytes())?;
    // f.write_all(out_forward.as_bytes())?;
    f.write_all(create_simple_forward_parametrized_delete_query(URI).as_bytes())?;

    Ok(())
}
