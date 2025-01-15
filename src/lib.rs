use anyhow::{format_err, Context, Error};
// use chrono::DateTime;
use futures03::StreamExt;
use lazy_static::lazy_static;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;
use regex::Regex;
use semver::Version;

use prost::Message;
use std::{env,  sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

// use pb::uniswap_types_v1::Pools;
use module_map::MODULES;

pub mod pb;
pub mod module_map;
pub mod substreams;
pub mod substreams_stream;

use reqwest::Client;
use serde_json::{json, Value};
// use std::error::Error;


lazy_static! {
    static ref MODULE_NAME_REGEXP: Regex = Regex::new(r"^([a-zA-Z][a-zA-Z0-9_-]{0,63})$").unwrap();
}

const REGISTRY_URL: &str = "https://spkg.io";

// Refactored into lib.rs
pub async fn run_substream(
    endpoint_url: String,
    package_file: &str,
    module_name: &str,
    range: Option<String>,
) -> Result<Vec<Vec<u8>>, Error> {
    let endpoint_url = if endpoint_url.starts_with("http") {
        endpoint_url
    } else {
        format!("https://{}", endpoint_url)
    };

    let token = env::var("SUBSTREAMS_API_TOKEN")
        .context("The environment variable SUBSTREAMS_API_TOKEN is not set")?;

    let package = read_package(package_file).await?;
    let block_range = read_block_range(&package, module_name, range.as_deref())?;
    let endpoint = Arc::new(SubstreamsEndpoint::new(&endpoint_url, Some(token)).await?);
    let cursor: Option<String> = load_persisted_cursor()?;
    let mut stream = SubstreamsStream::new(
        endpoint,
        cursor,
        package.modules,
        module_name.to_string(),
        block_range.0,
        block_range.1,
    );

    let mut results = Vec::new();

    while let Some(result) = stream.next().await {
        match result {
            Ok(BlockResponse::New(data)) => {
                match process_block_scoped_data(&data, &module_name) {
                    Ok(decoded_data) => results.push(decoded_data),
                    Err(err) => {
                        if err.to_string() == "Empty block data" {
                            // Skip empty blocks
                            continue;
                        } else {
                            return Err(err); // Propagate other errors
                        }
                    }
                }
                persist_cursor(data.cursor)?;
            }
            Ok(BlockResponse::Undo(undo_signal)) => {
                process_block_undo_signal(&undo_signal)?;
                persist_cursor(undo_signal.last_valid_cursor)?;
            }
            Err(err) => {
                return Err(anyhow::anyhow!("Stream terminated with error: {:?}", err));
            }
        }
    }

    Ok(results)
}

pub async fn rpc_call(
    rpc_endpoint: &str,
    method: &str,
    params_input: &str,
    id: i32,
) -> Result<Value, anyhow::Error> {
    // Ensure the endpoint starts with HTTP
    let rpc_endpoint = if rpc_endpoint.starts_with("http") {
        rpc_endpoint.to_string()
    } else {
        format!("https://{}", rpc_endpoint)
    };

    // Parse the parameters JSON
    let params: Value = serde_json::from_str(params_input)
        .context("Invalid JSON for parameters")?;

    // Build the JSON-RPC request body
    let request_body = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": id
    });

    // Create an HTTP client
    let client = Client::new();

    // Send the POST request
    let response = client
        .post(&rpc_endpoint)
        .json(&request_body)
        .send()
        .await
        .context("Failed to send RPC call")?;

    // Parse and return the response
    let response_json: Value = response
        .json()
        .await
        .context("Failed to parse RPC response")?;

    Ok(response_json)
}

pub async fn api_call(
    api_url: &str,
    optional_headers: Option<&str>,
) -> Result<String, anyhow::Error> {
    // Ensure the API URL starts with HTTP
    let api_url = if api_url.starts_with("http") {
        api_url.to_string()
    } else {
        format!("https://{}", api_url)
    };

    // Parse optional headers JSON
    let headers: Value = match optional_headers {
        Some(headers_str) => serde_json::from_str(headers_str)
            .context("Invalid JSON for headers")?,
        None => json!({}),
    };

    // Create HTTP client
    let client = Client::new();

    // Create request with optional headers
    let mut request = client.get(&api_url);
    if let Some(headers_map) = headers.as_object() {
        for (key, value) in headers_map {
            if let Some(header_value) = value.as_str() {
                request = request.header(key, header_value);
            }
        }
    }

    // Send the GET request
    let response = request
        .send()
        .await
        .context("Failed to send API call")?;

    // Parse and return the response text
    let response_text = response
        .text()
        .await
        .context("Failed to read API response")?;

    Ok(response_text)
}



// fn process_block_scoped_data(
//     data: &BlockScopedData,
//     module_name: &str,
// ) -> Result<Vec<u8>, Error> {
//     let output = data.output.as_ref().unwrap().map_output.as_ref().unwrap();

//     // Get the decoder for the module
//     // let decoder = MODULES
//     //     .get(module_name)
//     //     .ok_or_else(|| anyhow::anyhow!("Unknown module: {}", module_name))?;

//     if output.value.is_empty() {
//         // Return a specific error for empty blocks
//         return Err(anyhow::anyhow!("Empty block data"));
//     }

//     // Decode the value
//     // let decoded_value = decoder(output.value.as_slice())?;

//     Ok(output.value.clone())
// }

fn process_block_scoped_data(
    data: &BlockScopedData,
    _module_name: &str,
) -> Result<Vec<u8>, Error> {
    let output = data.output.as_ref().unwrap().map_output.as_ref().unwrap();

    if output.value.is_empty() {
        return Err(anyhow::anyhow!("Empty block data"));
    }

    // Ensure it directly returns Vec<u8>
    Ok(output.value.clone())
}


fn process_block_undo_signal(_undo_signal: &BlockUndoSignal) -> Result<(), anyhow::Error> {
    // `BlockUndoSignal` must be treated as "delete every data that has been recorded after
    // block height specified by block in BlockUndoSignal". In the example above, this means
    // you must delete changes done by `Block #7b` and `Block #6b`. The exact details depends
    // on your own logic. If for example all your added record contain a block number, a
    // simple way is to do `delete all records where block_num > 5` which is the block num
    // received in the `BlockUndoSignal` (this is true for append only records, so when only `INSERT` are allowed).
    unimplemented!("you must implement some kind of block undo handling, or request only final blocks (tweak substreams_stream.rs)")
}

fn persist_cursor(_cursor: String) -> Result<(), anyhow::Error> {
    // FIXME: Handling of the cursor is missing here. It should be saved each time
    // a full block has been correctly processed/persisted. The saving location
    // is your responsibility.
    //
    // By making it persistent, we ensure that if we crash, on startup we are
    // going to read it back from database and start back our SubstreamsStream
    // with it ensuring we are continuously streaming without ever losing a single
    // element.
    Ok(())
}

fn load_persisted_cursor() -> Result<Option<String>, anyhow::Error> {
    // FIXME: Handling of the cursor is missing here. It should be loaded from
    // somewhere (local file, database, cloud storage) and then `SubstreamStream` will
    // be able correctly resume from the right block.
    Ok(None)
}

fn read_block_range(
    pkg: &Package,
    module_name: &str,
    range: Option<&str>,
) -> Result<(i64, u64), anyhow::Error> {
    let module = pkg
        .modules
        .as_ref()
        .unwrap()
        .modules
        .iter()
        .find(|m| m.name == module_name)
        .ok_or_else(|| format_err!("module '{}' not found in package", module_name))?;

    let input: String = range.unwrap_or("").to_string();

    let (prefix, suffix) = match input.split_once(":") {
        Some((prefix, suffix)) => (prefix.to_string(), suffix.to_string()),
        None => ("".to_string(), input),
    };

    let start: i64 = match prefix.as_str() {
        "" => module.initial_block as i64,
        x if x.starts_with("+") => {
            let block_count = x
                .trim_start_matches("+")
                .parse::<u64>()
                .context("argument <start> is not a valid integer")?;

            (module.initial_block + block_count) as i64
        }
        x => x
            .parse::<i64>()
            .context("argument <start> is not a valid integer")?,
    };

    let stop: u64 = match suffix.as_str() {
        "" => 0,
        "-" => 0,
        x if x.starts_with("+") => {
            let block_count = x
                .trim_start_matches("+")
                .parse::<u64>()
                .context("argument <stop> is not a valid integer")?;

            start as u64 + block_count
        }
        x => x
            .parse::<u64>()
            .context("argument <stop> is not a valid integer")?,
    };

    Ok((start, stop))
}


async fn read_package(input: &str) -> Result<Package, anyhow::Error> {
    let mut mutable_input = input.to_string();

    let val = parse_standard_package_and_version(input);
    if val.is_ok() {
        let package_and_version = val.unwrap();
        mutable_input = format!(
            "{}/v1/packages/{}/{}",
            REGISTRY_URL, package_and_version.0, package_and_version.1
        );
    }

    if mutable_input.starts_with("http") {
        return read_http_package(&mutable_input).await;
    }

    // Assume it's a local file
    let content = std::fs::read(&mutable_input)
        .context(format_err!("read package from file '{}'", mutable_input))?;
    Package::decode(content.as_ref()).context("decode command")
}
async fn read_http_package(input: &str) -> Result<Package, anyhow::Error> {
    let body = reqwest::get(input).await?.bytes().await?;

    Package::decode(body).context("decode command")
}

fn parse_standard_package_and_version(input: &str) -> Result<(String, String), Error> {
    let parts: Vec<&str> = input.split('@').collect();
    if parts.len() > 2 {
        return Err(format_err!(
            "package name: {} does not follow the convention of <package>@<version>",
            input
        ));
    }

    let package_name = parts[0].to_string();
    if !MODULE_NAME_REGEXP.is_match(&package_name) {
        return Err(format_err!(
            "package name {} does not match regexp {}",
            package_name,
            MODULE_NAME_REGEXP.as_str()
        ));
    }

    if parts.len() == 1
        || parts
            .get(1)
            .map_or(true, |v| v.is_empty() || *v == "latest")
    {
        return Ok((package_name, "latest".to_string()));
    }

    let version = parts[1];
    if !is_valid_version(&version.replace("v", "")) {
        return Err(format_err!(
            "version '{}' is not valid Semver format",
            version
        ));
    }

    Ok((package_name, version.to_string()))
}

fn is_valid_version(version: &str) -> bool {
    Version::parse(version).is_ok()
}
