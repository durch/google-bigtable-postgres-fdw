extern crate gcc;
extern crate bindgen;

use std::io::prelude::*;
use std::fs::File;
use std::process::Command;
use std::path::PathBuf;

#[derive(Default)]
struct PGConfig {
    includedir: String,
    includedir_server: String,
    libdir: String
}

fn pg_config() -> PGConfig {
    let output = Command::new("pg_config").output().unwrap_or_else(|e| {
        panic!("Failed to execute process: {}", e)
    });
    /* Sample result:
        ...
        INCLUDEDIR = /usr/local/Cellar/postgresql/9.4.5/include
        PKGINCLUDEDIR = /usr/local/Cellar/postgresql/9.4.5/include
        INCLUDEDIR-SERVER = /usr/local/Cellar/postgresql/9.4.5/include/server
        LIBDIR = /usr/local/Cellar/postgresql/9.4.5/lib
        ...
     */

    let mut config = PGConfig { ..Default::default() };

    let text = String::from_utf8(output.stdout)
        .expect("Expected UTF-8 from call to `pg_config`.");

    for words in text.lines().map(|line| line.split_whitespace()) {
        let vec: Vec<&str> = words.collect();
        match vec[0] {
            "INCLUDEDIR" => config.includedir = vec[2].into(),
            "INCLUDEDIR-SERVER" => config.includedir_server = vec[2].into(),
            "LIBDIR" => config.libdir = vec[2].into(),
            _ => {}
        }
    }

    config
}

fn main() {
    let path = PathBuf::from("src");
    let config = pg_config();

    gcc::Config::new()
        .file(path.join("bt_fdw.c"))
        .include(&config.includedir)
        .include(&config.includedir_server)
        .compile("libmagic.a");
    // The GCC module emits `rustc-link-lib=static=magic` for us.

    // Also generate pg bindings
    let expanded = gcc::Config::new()
        .file(path.join("pgheaders.h"))
        .include(&config.includedir_server)
        .expand();

    let expanded_path = path.join("pgexpanded.h");
    let mut f = File::create(&expanded_path).expect(&format!("Could not create file: {:?}", expanded_path));
    f.write_all(&expanded.as_slice()).unwrap();

    let bindings = bindgen::Builder::default()
        // Do not generate unstable Rust code that
        // requires a nightly rustc and enabling
        // unstable features.
        .no_unstable_rust()
        .emit_builtins()
        // The input header we would like to generate
        // bindings for.
        .header(expanded_path.to_str().unwrap())
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(path.join("pg.rs"))
        .expect("Couldn't write bindings!");
}