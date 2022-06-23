const duckdb = require('duckdb');
const db = new duckdb.Database('noaa-cdo-small');

const con = db.connect();

con.all(`CREATE TABLE IF NOT EXISTS noaa AS SELECT * FROM 'noaa-sample.parquet'`, (err, res) => {
  console.log('Err:', err);
  console.log('Res:', res);
});

con.run(`CREATE TABLE IF NOT EXISTS stations AS SELECT * FROM 'noaa-gsn-stations.parquet'`, (err, res) => {
  console.log('Err:', err);
  console.log('Res:', res);
});
