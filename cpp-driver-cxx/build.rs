fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("src/tds_connection.cc")
        .include("include")
        .include(".")
        .flag_if_supported("-std=c++11")
        .compile("tds");

    println!("cargo::warning=CC: {:?}", std::env::var_os("CC"));
    println!("cargo::warning=OUT_DIR: {:?}", std::env::var_os("OUT_DIR"));
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/api_wrapper.rs");
    println!("cargo:rerun-if-changed=src/tds_connection.cc");
    println!("cargo:rerun-if-changed=include/tds_connection.h");
}
