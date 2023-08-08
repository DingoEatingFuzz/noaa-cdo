use chrono::prelude::*;
use chrono::{DateTime, LocalResult};
use clap::{Parser, ValueHint};
use csv::Writer;
use rayon::prelude::*;
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

    // Date type of the weather reading
    date: DateTime<Utc>,

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
        let date = Utc.ymd_opt(year, month.try_into().unwrap(), day.try_into().unwrap());

        let mflag = chars.next().unwrap();
        let qflag = chars.next().unwrap();
        let sflag = chars.next().unwrap();

        match date {
            LocalResult::Ambiguous(d1, d2) => {
                println!(
                    "Ambiguous Date for {} on: {}/{}/{} ({} or {})",
                    id, year, month, day, d1, d2
                )
            }
            LocalResult::None => {
                // We expect to get invalid dates based on the fixed days file format (e.g, Feb 30)
            }
            LocalResult::Single(d) => {
                days.push(CDO {
                    id: id.clone(),
                    year,
                    month,
                    day,
                    date: d.and_hms(0, 0, 0),
                    element: element.clone(),
                    value: value_str.trim().parse::<i32>().unwrap(),
                    mflag,
                    qflag,
                    sflag,
                });
            }
        }
    }

    return days;
}

fn parse_stations_line(line: String) -> Station {
    let mut chars = line.chars();
    let id: String = chars.by_ref().take(11).collect::<String>();

    chars.nth(0);
    let lat: f32 = chars
        .by_ref()
        .take(8)
        .collect::<String>()
        .trim()
        .parse::<f32>()
        .unwrap();

    chars.nth(0);
    let lon: f32 = chars
        .by_ref()
        .take(8)
        .collect::<String>()
        .trim()
        .parse::<f32>()
        .unwrap();

    chars.nth(0);
    let elevation: f32 = chars
        .by_ref()
        .take(7)
        .collect::<String>()
        .trim()
        .parse::<f32>()
        .unwrap();

    chars.nth(0);
    let state: String = String::from(chars.by_ref().take(2).collect::<String>().trim());

    chars.nth(0);
    let name: String = String::from(chars.by_ref().take(30).collect::<String>().trim());

    chars.nth(0);
    let gsn: bool = chars.by_ref().take(3).collect::<String>() == "GSN";

    chars.nth(0);
    let hcn_crn: String = chars.by_ref().take(3).collect::<String>();

    chars.nth(0);
    let wmo: String = String::from(chars.by_ref().take(5).collect::<String>().trim());

    return Station {
        id,
        lat,
        lon,
        elevation,
        state,
        name,
        gsn,
        hcn: hcn_crn == "HCN",
        crn: hcn_crn == "CRN",
        wmo,
    };
}

fn parse_cdo_file(file: PathBuf) -> Result<Vec<CDO>, Error> {
    let mut months = Vec::new();
    let f = File::open(file.as_path())?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        months.append(&mut parse_cdo_line(line.unwrap()));
    }
    return Ok(months);
}

fn parse_cdo(input: PathBuf) -> Result<(), Error> {
    let paths = fs::read_dir(input).unwrap();

    let mut files = Vec::new();

    // Qualify files in a directory
    for path in paths {
        let file = path.unwrap().path();
        if file.is_file() && file.extension().map(|s| s == "dly").unwrap_or(false) {
            files.push(file);
        }
    }

    let count = files.len();
    println!("Found {} .dly files", count);

    // Parse each file
    if Path::new("noaa-cdo.csv").exists() {
        fs::remove_file("noaa-cdo.csv")?;
    }
    let buffer = BufWriter::new(File::create("noaa-cdo.csv")?);
    let mut wtr = Writer::from_writer(buffer);

    let parsed = files
        .par_iter()
        .map(|f| -> Result<Vec<CDO>, Error> {
            println!("File {}", f.as_path().display());
            parse_cdo_file(f.to_path_buf())
        })
        .collect::<Vec<Result<Vec<CDO>, Error>>>();
    for (i, f) in parsed.iter().enumerate() {
        let fl = f.as_ref().unwrap();
        println!("Writing file {} of {}", i + 1, count);
        for r in fl {
            wtr.serialize(r)?;
        }
    }

    Ok(())
}

fn parse_stations(input: PathBuf) -> Result<(), Error> {
    if !input.is_file() {
        panic!("When using --stations, a file must be specified, not a directory");
    }

    if Path::new("noaa-stations.csv").exists() {
        fs::remove_file("noaa-stations.csv")?;
    }

    // Parse file
    let mut stations = Vec::<Station>::new();
    let f = File::open(input.as_path())?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        stations.push(parse_stations_line(line.unwrap()));
    }

    // Write to stations.csv
    let buffer = BufWriter::new(File::create("noaa-stations.csv")?);
    let mut wtr = Writer::from_writer(buffer);
    for s in stations {
        wtr.serialize(s)?;
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let opts: Opts = Opts::parse();

    if opts.stations {
        parse_stations(opts.input)?;
    } else {
        parse_cdo(opts.input)?;
    }

    Ok(())
}
