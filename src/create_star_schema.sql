-- Dim Airport
DROP VIEW IF EXISTS dim_airport;
CREATE VIEW dim_airport AS
SELECT DISTINCT
  airport,
  airport_name
FROM airline_delay_cleaned
WHERE airport IS NOT NULL;

-- Dim Date
DROP VIEW IF EXISTS dim_date;
CREATE VIEW dim_date AS
SELECT DISTINCT
  date(year || '-' || substr('00' || month, -2) || '-01') AS year_month,
  CAST(year  AS INTEGER) AS year,
  CAST(month AS INTEGER) AS month,
  strftime('%Y-%m', date(year || '-' || substr('00' || month, -2) || '-01')) AS ym_label,
  strftime('%m',     date(year || '-' || substr('00' || month, -2) || '-01')) AS month_num_txt,
  substr('JanFebMarAprMayJunJulAugSepOctNovDec',
         (CAST(strftime('%m', date(year || '-' || substr('00' || month, -2) || '-01')) AS INTEGER)-1)*3+1, 3) AS month_name
FROM airline_delay_cleaned
WHERE year IS NOT NULL AND month IS NOT NULL;


DROP VIEW IF EXISTS fact_delay;

CREATE VIEW fact_delay AS
WITH base AS (
  SELECT
    date(year || '-' || substr('00' || month, -2) || '-01') AS year_month,
    airport,
    carrier,
    CAST(arr_flights          AS REAL) AS arr_flights,
    CAST(arr_del15            AS REAL) AS arr_del15,
    CAST(arr_cancelled        AS REAL) AS arr_cancelled,
    CAST(arr_diverted         AS REAL) AS arr_diverted,
    CAST(arr_delay            AS REAL) AS arr_delay,
    CAST(carrier_ct           AS REAL) AS carrier_ct,
    CAST(weather_ct           AS REAL) AS weather_ct,
    CAST(nas_ct               AS REAL) AS nas_ct,
    CAST(security_ct          AS REAL) AS security_ct,
    CAST(late_aircraft_ct     AS REAL) AS late_aircraft_ct,
    CAST(carrier_delay        AS REAL) AS carrier_delay,
    CAST(weather_delay        AS REAL) AS weather_delay,
    CAST(nas_delay            AS REAL) AS nas_delay,
    CAST(security_delay       AS REAL) AS security_delay,
    CAST(late_aircraft_delay  AS REAL) AS late_aircraft_delay
  FROM airline_delay_cleaned
  WHERE year IS NOT NULL AND month IS NOT NULL
)
SELECT
  year_month, airport, carrier,
  SUM(arr_flights)   AS arr_flights,
  SUM(arr_del15)     AS arr_del15,
  SUM(arr_cancelled) AS arr_cancelcelled,
  SUM(arr_diverted)  AS arr_diverted,
  SUM(arr_delay)     AS arr_delay,
  SUM(carrier_ct)        AS carrier_ct,
  SUM(weather_ct)        AS weather_ct,
  SUM(nas_ct)            AS nas_ct,
  SUM(security_ct)       AS security_ct,
  SUM(late_aircraft_ct)  AS late_aircraft_ct,
  SUM(carrier_delay)        AS carrier_delay,
  SUM(weather_delay)        AS weather_delay,
  SUM(nas_delay)            AS nas_delay,
  SUM(security_delay)       AS security_delay,
  SUM(late_aircraft_delay)  AS late_aircraft_delay,
  CAST(SUM(arr_del15)     AS REAL) / NULLIF(SUM(arr_flights), 0)                                      AS delayed_rate,
  CAST(SUM(arr_cancelled) AS REAL) / NULLIF(SUM(arr_flights)+SUM(arr_cancelled)+SUM(arr_diverted), 0) AS cancellation_rate,
  CAST(SUM(arr_delay)     AS REAL) / NULLIF(SUM(arr_del15), 0)                                        AS avg_delay_min_per_delayed_flight
FROM base
GROUP BY year_month, airport, carrier;





