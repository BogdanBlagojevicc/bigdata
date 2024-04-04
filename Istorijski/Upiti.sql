-- 1. Mesec u kom je bilo najvise padavina u 2016 godini.
with PrecipitationPerMonthIn2016 as (
  select round(sum(fw.precipitation), 4) as total_precipitation, SUBSTRING(start_time, 1, 2) AS month
  from fact_weather fw LEFT OUTER JOIN dim_tyear dy on fw.dim_tyear_id_tyear = dy.id_tyear
  where dy.year = 2016
  group by SUBSTRING(start_time, 1, 2)
)

select *
from PrecipitationPerMonthIn2016 ppm2016
order by total_precipitation DESC
limit 1;

-- 2. Za svaku drzavu i za svaku godinu pronaci 2 vrste padavinina koje su su imale najvecu kolicinu pa zatim izracunati zbir tih padavina zaokruzen na 2 decimale.
with TopPrecipitationTypes as (
  select dy.year as year, fw.county, dt.name, RANK () OVER (PARTITION BY dy.year, fw.county ORDER BY SUM(fw.precipitation) DESC) AS precipitation_rank, SUM(fw.precipitation) as sumprecipitation
  from fact_weather as fw left outer join dim_tyear as dy on fw.dim_tyear_id_tyear = dy.id_tyear left outer join dim_type dt on fw.dim_type_id_type = dt.id_type
  group by dy.year, fw.county, dt.name
)
select tpt.county, tpt.year, round(SUM(tpt.sumprecipitation), 4)
from TopPrecipitationTypes tpt
where tpt.precipitation_rank < 3
group by tpt.county, tpt.year;

--3. Prikazati top 10 drzava u opadajucem redosledu koje imaju najvecu razliku ostvarenu izmedju 2 uzastopne godine u kolicini padavina zaokruzenu na 2 decimale
WITH PrecipitationChange as (
  select fw.county county, dy.year year, SUM(fw.precipitation) - LAG(SUM(fw.precipitation)) OVER (PARTITION BY fw.county ORDER BY dy.year) AS precipitation_increase
  from fact_weather fw left outer join dim_tyear dy on fw.dim_tyear_id_tyear = dy.id_tyear
  group by fw.county, dy.year
)
  select pc.county as county, ROUND(MAX(pc.precipitation_increase), 2) as max_increase
  from PrecipitationChange pc
  GROUP by pc.county
  order by max_increase DESC
  limit 10;

-- 4.Prikazati vremenske zone u 2017 godini u rastucem redosledu po najcescoj ozbiljnosti padaviva

WITH RankedSeverities AS ( 
    SELECT fw.dim_time_zone_id_tz, ds.name AS severity_name, COUNT(*) AS severity_count, ROW_NUMBER() OVER(PARTITION BY fw.dim_time_zone_id_tz ORDER BY COUNT(*) DESC) AS severity_rank
    FROM fact_weather fw
    JOIN dim_severity ds ON fw.dim_severity_id_severity = ds.id_severity
    JOIN dim_tyear dt ON fw.dim_tyear_id_tyear = dt.id_tyear
    WHERE dt.year = '2017'
    GROUP BY fw.dim_time_zone_id_tz, ds.name
)

SELECT rs.dim_time_zone_id_tz, rs.severity_name AS most_common_severity, rs.severity_count
FROM RankedSeverities rs
WHERE rs.severity_rank = 1
ORDER BY rs.dim_time_zone_id_tz ASC;

-- 5. Prikazati koja kombinacija tipa padavina i opasnosti je najcesca a da nije Light za svaku vremensku zonu pojednicacno
WITH RankedCombinations AS (
    SELECT 
        fw.dim_time_zone_id_tz, dt.name AS type_name, ds.name AS severity_name, COUNT(*) AS combination_count, RANK() OVER(PARTITION BY fw.dim_time_zone_id_tz ORDER BY COUNT(*) DESC) AS combination_rank
    FROM fact_weather fw
    JOIN dim_type dt ON fw.dim_type_id_type = dt.id_type JOIN dim_severity ds ON fw.dim_severity_id_severity = ds.id_severity
    WHERE ds.name != 'Light'
    GROUP BY fw.dim_time_zone_id_tz, dt.name, ds.name
)

SELECT rc.dim_time_zone_id_tz, dtz.name AS time_zone_name, rc.type_name, rc.severity_name, rc.combination_count
FROM RankedCombinations rc
JOIN dim_time_zone dtz ON rc.dim_time_zone_id_tz = dtz.id_tz
WHERE rc.combination_rank = 1;

-- 6. Grupisati u bakete vremenske zone i izracunati min, max, avg i count od kolicine padavina te prosek zaokruziti na 3 decimale
WITH PrecipitationBuckets AS (
    SELECT fw.dim_time_zone_id_tz, fw.event_id, fw.precipitation, NTILE(4) OVER(PARTITION BY fw.dim_time_zone_id_tz ORDER BY fw.precipitation) AS precipitation_bucket
    FROM fact_weather fw
)

SELECT pb.dim_time_zone_id_tz, dtz.name AS time_zone_name, pb.precipitation_bucket,
    MIN(pb.precipitation) AS min_precipitation,
    MAX(pb.precipitation) AS max_precipitation,
    ROUND(AVG(pb.precipitation), 3) AS avg_precipitation,
    COUNT(*) AS num_events
FROM PrecipitationBuckets pb
JOIN dim_time_zone dtz ON pb.dim_time_zone_id_tz = dtz.id_tz
GROUP BY pb.dim_time_zone_id_tz, dtz.name, pb.precipitation_bucket
ORDER BY pb.dim_time_zone_id_tz, pb.precipitation_bucket;

-- 7. Prikazati prosecnu kolicinu padavina za svaki dogadjaj posmatrajuci prethodna 3 dogadjaja unutar iste vremenske zone
WITH RollingAveragePrecipitation AS (
    SELECT fw.event_id, fw.dim_time_zone_id_tz, fw.precipitation, AVG(fw.precipitation) OVER(PARTITION BY fw.dim_time_zone_id_tz ORDER BY fw.event_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_precipitation
    FROM fact_weather fw
)

SELECT fw.event_id, dtz.name AS time_zone_name, fw.precipitation, round(rap.rolling_avg_precipitation,4)
FROM fact_weather fw
JOIN dim_time_zone dtz ON fw.dim_time_zone_id_tz = dtz.id_tz JOIN RollingAveragePrecipitation rap ON fw.event_id = rap.event_id;

--8. Pronaci sledecu ozbiljnost padavine u odnosu na trenutnu i oznaciti sa 0 u koliko se ne poklapaju odnosno sa 1 u koliko se poklapaju - 6min
SELECT fw.event_id, fw.city, fw.county, fw.state, ds.name AS current_severity_name, tz.name AS current_time_zone_name, 
      LEAD(fw.dim_severity_id_severity) OVER (ORDER BY fw.id_weather) AS next_severity_id, 
      COALESCE(same_severity_count, 0) AS same_next_severity_count
FROM fact_weather fw
JOIN dim_severity ds ON fw.dim_severity_id_severity = ds.id_severity JOIN dim_time_zone tz ON fw.dim_time_zone_id_tz = tz.id_tz
LEFT JOIN dim_severity ds_next ON fw.dim_severity_id_severity = ds_next.id_severity
LEFT JOIN (
    SELECT
        fw.id_weather,
        COUNT(*) AS same_severity_count
    FROM
        fact_weather fw
    JOIN
        fact_weather fw_next ON fw.id_weather = fw_next.id_weather - 1
    WHERE
        fw.dim_severity_id_severity = fw_next.dim_severity_id_severity
    GROUP BY
        fw.id_weather
) AS count_table ON fw.id_weather = count_table.id_weather;

-- 9. Prikazazati mesece koji imaju vecu kolicinu padavina od prosecne rangirane u rastucem redosledu po mesecu
WITH month_precipitation AS (
    SELECT SUBSTRING(start_time, 1, 2) AS month, SUM(precipitation) AS total_precipitation
    FROM fact_weather
    GROUP BY SUBSTRING(start_time, 1, 2)
),
ranked_precipitation AS (
    SELECT month, total_precipitation,ROW_NUMBER() OVER (ORDER BY total_precipitation DESC) AS precipitation_rank, AVG(total_precipitation) OVER () AS avg_precipitation
    FROM month_precipitation
)
SELECT month, round(total_precipitation, 4) as total_precipitation, precipitation_rank
FROM ranked_precipitation
WHERE total_precipitation > avg_precipitation
ORDER BY 
    CASE month
        WHEN '01' THEN 1
        WHEN '02' THEN 2
        WHEN '03' THEN 3
        WHEN '04' THEN 4
        WHEN '05' THEN 5
        WHEN '06' THEN 6
        WHEN '07' THEN 7
        WHEN '08' THEN 8
        WHEN '09' THEN 9
        WHEN '10' THEN 10
        WHEN '11' THEN 11
        WHEN '12' THEN 12
    END;

-- 10. Izracunaj broj pojavljivanja razlicitih ozbiljnosti padavina grupisanih u 4 baketa 
SELECT s.name AS severity_name,
       COUNT(*) AS count_in_group
FROM (
    SELECT dim_severity_id_severity, 
           NTILE(4) OVER (ORDER BY dim_severity_id_severity) AS quartile
    FROM fact_weather
) AS quartiles
JOIN dim_severity AS s ON quartiles.dim_severity_id_severity = s.id_severity
GROUP BY s.name;


-- PREDUGO - preko 15min - Racuna koliko se zaredom pojavljuju isti severity (ozbiljnosti padavina)
WITH weather_with_rank AS (
    SELECT
        fw.event_id,
        fw.city,
        fw.county,
        fw.state,
        ds.name AS current_severity_name,
        tz.name AS current_time_zone_name,
        LEAD(fw.dim_severity_id_severity) OVER (ORDER BY fw.id_weather) AS next_severity_id,
        ds_next.name AS next_severity_name,
        ROW_NUMBER() OVER (ORDER BY fw.id_weather) AS rn
    FROM
        fact_weather fw
    JOIN
        dim_severity ds ON fw.dim_severity_id_severity = ds.id_severity
    JOIN
        dim_time_zone tz ON fw.dim_time_zone_id_tz = tz.id_tz
    LEFT JOIN
        dim_severity ds_next ON fw.dim_severity_id_severity = ds_next.id_severity
)

SELECT
    event_id,
    city,
    county,
    state,
    current_severity_name,
    current_time_zone_name,
    COUNT(*) AS consecutive_events_with_same_severity
FROM (
    SELECT
        w.*,
        COUNT(CASE WHEN current_severity_name = next_severity_name THEN 1 END) OVER (ORDER BY rn) AS grp
    FROM
        weather_with_rank w
) grouped
GROUP BY
    event_id,
    city,
    county,
    state,
    current_severity_name,
    current_time_zone_name,
    grp;



