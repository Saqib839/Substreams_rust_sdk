## UnifiedStreams
This code contains robust library code for integrating Substreams, RPC, and API calls into your applications. It simplifies real-time data processing, cross-protocol communication, and API integrations. Designed with extensibility, performance, and reliability in mind, it’s perfect for building scalable, multi-protocol systems.

## Compiling the Library

1. **Clone the Repository and Checkout in `main` Branch**  
   ```bash git clone https://github.com/SentinelStrategies/UnifiedStreams.git ```
   
3. **Install Rust and Cargo**
Ensure you have both the Rust compiler (rustc) and Cargo package manager installed and configured in your system PATH.

4. **Install OpenSSL and Related Dependencies**
Make sure OpenSSL and any necessary dependencies are correctly installed and recognized by your system.

5. **Compile**
```bash cargo build --release ```
This will produce optimized binary artifacts in the target/release directory, including various library files. These files can be integrated into other languages or projects as needed.

## Usage

# 1. Setup a substreams JWT TOKEN
You can set a JWT token as an environment variable to avoid specifying it for each session. Replace <jwt_token_here> with your actual token from substreams.

```bash
      nano ~/.bashrc
      export SUBSTREAMS_API_TOKEN="jwt_token_here"
      source ~/.bashrc
```

# 2. Import the library in the corresponding language and call the corresponding function from the library to get results.
      rpc_call_ffi
      api_call_ffi
      substreams_call_ffi
