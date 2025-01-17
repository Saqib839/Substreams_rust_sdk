use std::ffi::{CString, CStr};
use std::os::raw::c_char;
use lazy_static::lazy_static;
use tokio::runtime::Runtime;
use crate::{rpc_call, api_call, substreams_call};

// Struct to represent raw byte array
#[repr(C)]
pub struct FfiByteArray {
    pub data: *mut u8,   // Pointer to the raw byte array
    pub length: usize,   // Length of the byte array
}

impl FfiByteArray {
    pub fn new(bytes: Vec<u8>) -> Self {
        let length = bytes.len();
        let data = bytes.as_ptr() as *mut u8;

        // Prevent Rust from freeing the memory
        std::mem::forget(bytes);

        Self { data, length }
    }
}

// String wrapper for C compatibility
pub struct FfiString {
    ptr: *mut c_char,
}

impl FfiString {
    pub fn new(s: String) -> Self {
        let c_string = CString::new(s).unwrap();
        let ptr = c_string.into_raw(); // Allocate memory
        Self { ptr }
    }

    pub fn as_ptr(&self) -> *mut c_char {
        self.ptr
    }
}

impl Drop for FfiString {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                let _ = CString::from_raw(self.ptr); // Deallocate the memory
            }
        }
    }
}

// Global tokio runtime
lazy_static! {
    static ref RUNTIME: Runtime = Runtime::new().unwrap();
}

// Substreams call for raw bytes
#[no_mangle]
pub extern "C" fn substreams_call_ffi(
    endpoint_url: *const c_char,
    package_file: *const c_char,
    module_name: *const c_char,
    range: *const c_char,
    out_length: *mut usize,
) -> *mut FfiByteArray {
    if endpoint_url.is_null() || package_file.is_null() || module_name.is_null() {
        return std::ptr::null_mut();
    }

    let endpoint_url = unsafe { CStr::from_ptr(endpoint_url).to_string_lossy().to_string() };
    let package_file = unsafe { CStr::from_ptr(package_file).to_string_lossy().to_string() };
    let module_name = unsafe { CStr::from_ptr(module_name).to_string_lossy().to_string() };
    let range = unsafe {
        if range.is_null() {
            None
        } else {
            Some(CStr::from_ptr(range).to_string_lossy().to_string())
        }
    };

    let result = RUNTIME.block_on(substreams_call(endpoint_url, &package_file, &module_name, range));

    match result {
        Ok(results) => {
            let mut ffi_results: Vec<FfiByteArray> = Vec::new();
            for bytes in results {
                ffi_results.push(FfiByteArray::new(bytes));
            }

            unsafe {
                if !out_length.is_null() {
                    *out_length = ffi_results.len();
                }
            }

            Box::into_raw(ffi_results.into_boxed_slice()) as *mut FfiByteArray
        }
        Err(_) => std::ptr::null_mut(),
    }
}


#[no_mangle]
pub extern "C" fn rpc_call_ffi(
    rpc_endpoint: *const c_char,
    method: *const c_char,
    params_input: *const c_char,
    id: i32,
) -> *mut c_char {
    println!("rpc_call_ffi: Starting");

    // Convert inputs
    let rpc_endpoint = unsafe { CStr::from_ptr(rpc_endpoint).to_string_lossy().to_string() };
    let method = unsafe { CStr::from_ptr(method).to_string_lossy().to_string() };
    let params_input = unsafe { CStr::from_ptr(params_input).to_string_lossy().to_string() };

    println!("rpc_call_ffi: Received rpc_endpoint={}, method={}, params_input={}, id={}",
        rpc_endpoint, method, params_input, id);

    // Perform the RPC call
    let result = RUNTIME.block_on(rpc_call(&rpc_endpoint, &method, &params_input, id));

    match result {
        Ok(json) => {
            let json_string = serde_json::to_string(&json).unwrap();
            println!("rpc_call_ffi: Successfully received JSON response: {}", json_string);
            FfiString::new(json_string).as_ptr()
        }
        Err(err) => {
            let error_message = format!("rpc_call_ffi: Error performing RPC call: {}", err);
            println!("{}", error_message);
            FfiString::new(error_message).as_ptr()
        }
    }
}


#[no_mangle]
pub extern "C" fn api_call_ffi(
    api_url: *const c_char,
    optional_headers: *const c_char,
) -> *mut c_char {
    if api_url.is_null() {
        return CString::new("Null pointer passed").unwrap().into_raw();
    }

    let api_url = unsafe { CStr::from_ptr(api_url).to_string_lossy().to_string() };
    let optional_headers = unsafe {
        if optional_headers.is_null() {
            None
        } else {
            Some(CStr::from_ptr(optional_headers).to_string_lossy().to_string())
        }
    };

    let result = RUNTIME.block_on(api_call(&api_url, optional_headers.as_deref()));

    match result {
        Ok(response) => CString::new(response).unwrap().into_raw(),
        Err(err) => CString::new(format!("Error: {}", err)).unwrap().into_raw(),
    }
}

// Free a string pointer
#[no_mangle]
pub extern "C" fn free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(ptr); // Safely deallocates the memory
    }
}

// Free a byte array
#[no_mangle]
pub extern "C" fn free_byte_array(ptr: *mut FfiByteArray, length: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::slice::from_raw_parts_mut(ptr, length);
        for array in &mut *slice {
            let _ = Vec::from_raw_parts(array.data, array.length, array.length);
        }
        let _ = Box::from_raw(slice);
    }
}