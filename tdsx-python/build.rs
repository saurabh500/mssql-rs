fn main() {
    eprintln!("----------------------------------------------------------");
    // Check if the target is x86_64-apple-darwin or aarch64-apple-darwin
    // https://github.com/PyO3/pyo3/issues/1330
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();

    if target_os == "macos" && (target_arch == "x86_64" || target_arch == "aarch64") {
        println!("cargo:rustc-link-arg=-undefined");
        println!("cargo:rustc-link-arg=dynamic_lookup");
    }
}
