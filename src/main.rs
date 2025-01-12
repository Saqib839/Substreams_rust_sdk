use anyhow::{format_err, Context, Error};
// use chrono::DateTime;
use futures03::StreamExt;
use lazy_static::lazy_static;
use pb::sf::substreams::rpc::v2::{BlockScopedData, BlockUndoSignal};
use pb::sf::substreams::v1::Package;
use regex::Regex;
use semver::Version;

use prost::Message;
use std::{env, process::exit, sync::Arc};
use substreams::SubstreamsEndpoint;
use substreams_stream::{BlockResponse, SubstreamsStream};

// use pb::uniswap_types_v1::Pools;
// use module_map::MODULES;

pub mod pb;
// mod pb;
mod substreams;
mod substreams_stream;
// mod module_map; 

use reqwest::Client;
use serde_json::{json, Value};
// use std::env;

lazy_static! {
    static ref MODULE_NAME_REGEXP: Regex = Regex::new(r"^([a-zA-Z][a-zA-Z0-9_-]{0,63})$").unwrap();
}

const REGISTRY_URL: &str = "https://spkg.io";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check if the first argument is either "rpc_call" or "substreams_call"
    if args.len() < 2 || (args[1] != "rpc_call" && args[1] != "substreams_call" && args[1] != "api_call")  {
        eprintln!("\nArguments error follow one of the format below!\n
                    Usage: cargo run api_call <api-url> [optional-headers]\n
                    Usage: cargo run rpc_call <rpc-url> <method> <params> <id>\n
                    Usage: cargo run substreams_call <stream endpoint> <spkg> <module> <start>:<stop>\n");
        return Ok(());
    }

    if args[1] == "rpc_call" {
        if args.len() != 6  {
            eprintln!("Unknown rpc_call arguments. Usage: cargo run rpc_call <rpc-url> <method> <params> <id>");
            return Ok(());
        }

        // Extract arguments for rpc_call
        let rpc_endpoint = &args[2];
        let method = &args[3];
        let params_input = &args[4];
        let id: i32 = args[5].parse().expect("Invalid ID (must be a number).");

        // Parse the parameters JSON
        let params: Value = serde_json::from_str(params_input).expect("Invalid JSON for parameters.");

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
            .post(rpc_endpoint)
            .json(&request_body)
            .send()
            .await?;

        // Parse and display the response
        let response_json: Value = response.json().await?;
        println!("Response: {}", serde_json::to_string_pretty(&response_json)?);

        return Ok(());
    }

    if args[1] == "substreams_call" {
        if args.len() != 5 && args.len() != 6 {
            println!("command format: cargo run substreams_call <stream endpoint> <spkg> <module> <start>:<stop>\n");
            println!("Ensure the environment variable SUBSTREAMS_API_TOKEN is set with a valid Substream API token.\n");
            println!("<spkg> can either be the full spkg.io link or `spkg_package@version`\n");
            println!("Example usage: stream mainnet.injective.streamingfast.io:443 injective-common@v0.2.3 all_events 1:10\n");
            return Ok(());
        }

        let mut endpoint_url = args[2].clone();
        let package_file = args[3].clone();
        let module_name = args[4].clone();
        // let block_range = args[5].clone();

        if !endpoint_url.starts_with("http") {
            endpoint_url = format!("{}://{}", "https", endpoint_url);
        }

        let token = env::var("SUBSTREAMS_API_TOKEN")
            .expect("The environment variable SUBSTREAMS_API_TOKEN is not set\n
                      Set using export SUBSTREAMS_API_TOKEN='eyJhbGciOiJL....'\n");

        let package = read_package(&package_file).await?;
        let block_range = read_block_range(&package, &module_name)?;

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

        loop {
            match stream.next().await {
                None => {
                    println!("Stream consumed");
                    break;
                }
                Some(Ok(BlockResponse::New(data))) => {
                    process_block_scoped_data(&data)?;
                    persist_cursor(data.cursor)?;
                }
                Some(Ok(BlockResponse::Undo(undo_signal))) => {
                    process_block_undo_signal(&undo_signal)?;
                    persist_cursor(undo_signal.last_valid_cursor)?;
                }
                Some(Err(err)) => {
                    println!();
                    println!("Stream terminated with error");
                    println!("{:?}", err);
                    exit(1);
                }
            }
        }

        return Ok(());
    }

    if args[1] == "api_call"{

        if args.len() != 3 && args.len() != 4  {
            eprintln!("Usage: cargo run api_call <api-url> [optional-headers]");
            return Ok(());
        }

        let api_url = &args[2];

        // Optional headers (passed as JSON string)
        // let optional_headers = args.get(3).unwrap_or(&"{}".to_string());
        // let headers: Value = serde_json::from_str(optional_headers).unwrap_or(Value::Null);

        let fallback_headers = "{}".to_string(); // Create a fallback string
        let optional_headers = args.get(3).unwrap_or(&fallback_headers); // Use the fallback if no argument is provided
        let headers: Value = serde_json::from_str(optional_headers).unwrap_or(Value::Null); // Parse headers JSON


        // Create HTTP client
        let client = Client::new();

        // Create request with optional headers
        let mut request = client.get(api_url);
        if let Some(headers_map) = headers.as_object() {
            for (key, value) in headers_map {
                if let Some(header_value) = value.as_str() {
                    request = request.header(key, header_value);
                }
            }
        }

        // Send request and handle response
        let response = request.send().await?;
        // let status = response.status();
        let response_text = response.text().await?;

        // println!("Status Code: {}", status);
        println!("Response: {}", response_text);
        }

    Ok(())
}

fn process_block_scoped_data(data: &BlockScopedData) -> Result<(), Error> {
    let output = data.output.as_ref().unwrap().map_output.as_ref().unwrap();

    // You can decode the actual Any type received using this code:
    //
    // let vas = output.value.as_slice();
    // println!("value {:?}", vas);

    // let value = Pools::decode(output.value.as_slice())?;

    // Get the decoder for the module
    // let decoder = MODULES
    //     .get(module_name)
    //     .ok_or_else(|| anyhow::anyhow!("Unknown module: {}", module_name))?;

    // let value = decoder(output.value.as_slice())?;
    
    //
    // Where GeneratedStructName is the Rust code generated for the Protobuf representing
    // your type, so you will need generate it using `substreams protogen` and import it from the
    // `src/pb` folder.

    // let clock = data.clock.as_ref().unwrap();
    // let timestamp = clock.timestamp.as_ref().unwrap();
    // let date = DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
    //     .expect("received timestamp should always be valid");


    if output.value.len() > 0 {
        println!("{:?}", output.value);  // For Debug output
        println!();
        // println!("Block #{} - Payload {} ({} bytes) - Drift {}s",
        //     clock.number,
        //     output.type_url.replace("type.googleapis.com/", ""),
        //     output.value.len(),
        //     date.signed_duration_since(chrono::offset::Utc::now())
        //         .num_seconds()
        //         * -1
        // );
    }   

    Ok(())
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

fn read_block_range(pkg: &Package, module_name: &str) -> Result<(i64, u64), anyhow::Error> {
    let module = pkg
        .modules
        .as_ref()
        .unwrap()
        .modules
        .iter()
        .find(|m| m.name == module_name)
        .ok_or_else(|| format_err!("module '{}' not found in package", module_name))?;

    let mut input: String = "".to_string();
    if let Some(range) = env::args().nth(5) {
        input = range;
    };

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
                .context("argument <stop> is not a valid integer")?;

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

    return Ok((start, stop));
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
