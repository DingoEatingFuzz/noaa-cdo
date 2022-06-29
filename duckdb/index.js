const duckdb = require('duckdb');
const cdoDB = new duckdb.Database('noaa-cdo');

const promisify = fn => {
  return new Promise((res, rej) => {
    fn((err, result) => {
      if (err) console.error(err) && rej(err);
      res(result);
    });
  });
}

async function weatherDataSamples(con) {
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

  await promisify(handler => {
    con.all(`CREATE TEMP TABLE sample AS (${recentDataQuery})`, handler);
  });
  console.log('Temp table created');

  const query = `COPY (SELECT * FROM sample) TO 'noaa-sample.parquet' (FORMAT 'parquet')`;
  console.log(await promisify(handler => con.all(query, handler)));
  console.log('noaa-sample.parquet created');
}

async function gsnStations(con) {
  const materializedStationsQuery = `
    SELECT s.*, (sam.id is NOT NULL) as sampled
    FROM stations s
    LEFT JOIN (SELECT DISTINCT id FROM sample) sam ON s.id = sam.id
    WHERE s.gsn = TRUE
  `;

  const query = `COPY (${materializedStationsQuery}) TO 'noaa-gsn-stations.parquet' (FORMAT 'parquet')`;

  console.log(await promisify(handler => { con.all(query, handler) }));
  console.log('noaa-gsn-stations.parquet created');
}

const connection = cdoDB.connect();

weatherDataSamples(connection).then(() => {
  return gsnStations(connection);
});
