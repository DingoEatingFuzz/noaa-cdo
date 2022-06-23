use clap::{Parser, ValueHint};
use csv::Writer;
use serde::Serialize;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Error};
use std::path::{Path, PathBuf};
use std::vec::Vec;

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = "Michael Lange <mlange.cs@gmail.com>")]
struct Opts {
    #[clap(name = "Directory", parse(from_os_str), value_hint = ValueHint::DirPath)]
    input: PathBuf,

    #[clap(short, long)]
    stations: bool,
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

// ------------------------------
// Variable   Columns   Type
// ------------------------------
// ID            1-11   Character
// LATITUDE     13-20   Real
// LONGITUDE    22-30   Real
// ELEVATION    32-37   Real
// STATE        39-40   Character
// NAME         42-71   Character
// GSN FLAG     73-75   Character
// HCN/CRN FLAG 77-79   Character
// WMO ID       81-85   Character
// ------------------------------

#[derive(Serialize)]
struct Station {
    id: String,
    lat: f32,
    lon: f32,
    elevation: f32,
    state: String,
    name: String,
    gsn: bool,
    hcn: bool,
    crn: bool,
    wmo: String,
}

fn parse_cdo_line(line: String) -> Vec<CDO> {
    let mut days = Vec::new();
    let mut chars = line.chars();

    // Common values for all days
    let id: String = chars.by_ref().take(11).collect::<String>();
    let year: i32 = chars
        .by_ref()
        .take(4)
        .collect::<String>()
        .trim()
        .parse::<i32>()
        .unwrap();
    let month: i32 = chars
        .by_ref()
        .take(2)
        .collect::<String>()
        .trim()
        .parse::<i32>()
        .unwrap();
    let element: String = chars.by_ref().take(4).collect::<String>();

    for day in 1..31 {
        let value_str = chars.by_ref().take(5).collect::<String>();
        days.push(CDO {
            id: id.clone(),
            year: year,
            month: month,
            element: element.clone(),
            day: day,
            value: value_str.trim().parse::<i32>().unwrap(),
            mflag: chars.next().unwrap(),
            qflag: chars.next().unwrap(),
            sflag: chars.next().unwrap(),
        });
    }

    return days;
}

fn parse_cdo_file(file: PathBuf) -> Result<Vec<CDO>, Error> {
    println!("File: {}", file.as_path().display());
    let mut months = Vec::new();
    let f = File::open(file.as_path())?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        months.append(&mut parse_cdo_line(line.unwrap()));
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
    if Path::new("noaa-cdo.csv").exists() {
        fs::remove_file("noaa-cdo.csv")?;
    }
    let buffer = BufWriter::new(File::create("noaa-cdo.csv")?);
    let mut wtr = Writer::from_writer(buffer);

    for f in files {
        for r in parse_cdo_file(f.to_path_buf())? {
            wtr.serialize(r)?;
        }
    }

    Ok(())
}
