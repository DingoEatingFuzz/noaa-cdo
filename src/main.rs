use clap::{Parser, ValueHint};
use csv::Writer;
use serde::Serialize;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Error};
use std::path::PathBuf;
use std::vec::Vec;

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Michael Lange <mlange.cs@gmail.com>")]
struct Opts {
    #[clap(name = "Directory", parse(from_os_str), value_hint = ValueHint::DirPath)]
    input: PathBuf,
}

// ------------------------------
// Variable   Columns   Type
// ------------------------------
// ID            1-11   Character
// YEAR         12-15   Integer
// MONTH        16-17   Integer
// ELEMENT      18-21   Character
// VALUE1       22-26   Integer
// MFLAG1       27-27   Character
// QFLAG1       28-28   Character
// SFLAG1       29-29   Character
// VALUE2       30-34   Integer
// MFLAG2       35-35   Character
// QFLAG2       36-36   Character
// SFLAG2       37-37   Character
//   .           .          .
//   .           .          .
//   .           .          .
// VALUE31    262-266   Integer
// MFLAG31    267-267   Character
// QFLAG31    268-268   Character
// SFLAG31    269-269   Character

#[derive(Serialize)]
struct CDO {
    // ID of the weather station
    id: String,

    // Date of the weather reading
    year: i32,
    month: i32,
    day: i32,

    // The element type (there are five core elements + some additional ones)
    element: String,

    // The value of the element (-9999 means missing)
    value: i32,

    // Measurement flag
    mflag: char,

    // Quality flag
    qflag: char,

    // Source flag
    sflag: char,
}

fn parse_line(line: String) -> Vec<CDO> {
    let days = Vec::new();
    println!("Line: {line}");
    return days;
}

fn parse_file(file: PathBuf) -> Result<Vec<CDO>, Error> {
    let mut months = Vec::new();
    let f = File::open(file.as_path())?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        months.append(&mut parse_line(line.unwrap()));
    }
    return Ok(months);
}

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();
    let paths = fs::read_dir(opts.input).unwrap();

    let mut files = Vec::new();

    // Qualify files in a directory
    for path in paths {
        let file = path.unwrap().path();
        if file.is_file() && file.extension().map(|s| s == "dly").unwrap_or(false) {
            files.push(file);
        }
    }

    println!("Found {} .dly files", files.len());

    // Parse each file
    let buffer = BufWriter::new(File::create("noaa-cdo.csv")?);
    let mut wtr = Writer::from_writer(buffer);

    // Write headers
    wtr.write_record(&[
        "ID", "Year", "Month", "Day", "Element", "Value", "MFlag", "QFlag", "SFlag",
    ])?;

    for f in files.iter().take(3) {
        for r in parse_file(f.to_path_buf()) {
            wtr.serialize(r)?;
        }
    }

    Ok(())
}
