const duckdb = require('duckdb');
const db = new duckdb.Database('noaa-cdo');

const con = db.connect();

con.run(`CREATE TABLE IF NOT EXISTS noaa AS SELECT * FROM '../noaa-cdo.parquet'`);
con.run(`CREATE TABLE IF NOT EXISTS stations AS SELECT * FROM '../noaa-stations.parquet'`);
// Ideally this happens by default via the parquet import
con.run(`ALTER TABLE noaa ALTER date TYPE DATE;`);
