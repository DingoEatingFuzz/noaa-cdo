const duckdb = require('duckdb');
const cdoDB = new duckdb.Database('noaa-cdo');

function weatherDataSamples(db) {
  const recentDataQuery = `
    SELECT * FROM noaa
    WHERE year >= 2010
      AND element in ('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN', 'TAVG', 'AWND', 'AWDR')
      AND id in (
        SELECT id from (
          SELECT id FROM noaa
          GROUP BY id
        )
        USING SAMPLE 100
      )
  `;

  const query = `COPY (${recentDataQuery}) TO 'noaa-sample.parquet' (FORMAT 'parquet')`;

  db.all(query, (err, res) => {
    if (err) console.log(err);
    console.log(res);
  });
}

function gsnStations(db) {
  const gsnStationsQuery = 'SELECT * FROM stations WHERE gsn = true';

  const query = `COPY (${gsnStationsQuery}) TO 'noaa-gsn-stations.parquet' (FORMAT 'parquet')`;

  db.all(query, (err, res) => {
    if (err) console.log(err);
    console.log(res);
  });
}

weatherDataSamples(cdoDB);
gsnStations(cdoDB);
