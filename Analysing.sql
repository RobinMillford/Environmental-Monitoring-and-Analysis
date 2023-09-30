use portfolioprojects;

select * from  iot_telemetry_data;

-- 1. the average temperature recorded for each device.
select device, AVG(temp) as average_temperature
from iot_telemetry_data
group by device;

-- 2. the devices with the highest average carbon monoxide levels:
select
device,avg(co) as average_carbon_monoxide
from iot_telemetry_data
group by device
order by average_carbon_monoxide desc;

-- 3. the average temperature recorded
select avg(temp) as average_temperature
from iot_telemetry_data;

-- 4. the timestamp and temperature of the highest recorded temperature for each device
SELECT c.device, c.ts, c.temp
FROM iot_telemetry_data c
JOIN (
    SELECT device, MAX(temp) AS max_temp
    FROM iot_telemetry_data
    GROUP BY device
) t ON c.device = t.device AND c.temp = t.max_temp;

--  5. devices where the temperature has increased from the minimum recorded temperature to the maximum recorded temperature
select device
from iot_telemetry_data
group by device
having min(temp) < max(temp);

-- 6. the exponential moving average (EMA) of the temperature for each device and retrieve the device ID, timestamp, temperature, and EMA temperature for the first 10 devices from the table

SELECT ts, device, temp, ema_temperature
FROM (
    SELECT ts, device, temp,
           AVG(temp) OVER (PARTITION BY device ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema_temperature,
           ROW_NUMBER() OVER (PARTITION BY device ORDER BY ts) AS rn
    FROM iot_telemetry_data
) subquery
WHERE rn <= 10;

-- 7. the timestamps and devices where the carbon monoxide level exceeds the average carbon monoxide level across all devices
SELECT ts, device, co
FROM iot_telemetry_data
WHERE co > (SELECT AVG(co) FROM iot_telemetry_data);

-- 8. the highest average temperature recorded
select device, avg(temp) as average_temperature
from iot_telemetry_data
group by device
having avg(temp) = (
select max(avg_temp)
from(
select avg(temp) as avg_temp
from iot_telemetry_data
group by device
) as temp_avg 
);

-- 9. the average temperature for each hour of the day across all devices
SELECT 
    DATEPART(HOUR, DATEADD(SECOND, CAST(ts AS FLOAT), '19700101 00:00:00')) AS hour_of_day,
    AVG(temp) AS average_temperature
FROM iot_telemetry_data
WHERE ISNUMERIC(ts) = 1 -- Filter out non-numeric timestamp values
GROUP BY DATEPART(HOUR, DATEADD(SECOND, CAST(ts AS FLOAT), '19700101 00:00:00'))
ORDER BY hour_of_day;

-- 10. a single distinct temperature value
SELECT device, COUNT(DISTINCT temp) AS distinct_temp_count, COUNT(*) AS total_records
FROM iot_telemetry_data
GROUP BY device

-- 11. the devices with the highest humidity levels
select device, max(humidity) as highest_humidity
from iot_telemetry_data
group by device;

-- 12. the average temperature for each device, excluding outliers (temperatures beyond 3 standard deviations)
SELECT device, AVG(temp) AS average_temperature
FROM (
    SELECT device, temp,
           STDEV(temp) OVER (PARTITION BY device) AS stddev,
           AVG(temp) OVER (PARTITION BY device) AS avg_temp
    FROM iot_telemetry_data
) subquery
WHERE temp BETWEEN (avg_temp - 3 * stddev) AND (avg_temp + 3 * stddev)
GROUP BY device;

-- 13. the devices that have experienced a sudden change in humidity (greater than 50% difference) within a 30-minute window based on the given column names
WITH HumidityChanges AS (
    SELECT
        device,
        DATEADD(SECOND, ts, '19700101 00:00:00') AS datetime,
        LAG(DATEADD(SECOND, ts, '19700101 00:00:00')) OVER (PARTITION BY device ORDER BY DATEADD(SECOND, ts, '19700101 00:00:00')) AS prev_datetime,
        humidity
    FROM
        iot_telemetry_data
)
SELECT DISTINCT device
FROM (
    SELECT
        H.device,
        ABS(H.humidity - LAG(H.humidity) OVER (PARTITION BY H.device ORDER BY H.datetime)) AS humidity_change,
        DATEDIFF(SECOND, H.prev_datetime, H.datetime) AS time_difference
    FROM
        HumidityChanges H
) AS subquery
WHERE humidity_change > 0.5 * (SELECT MAX(humidity) FROM iot_telemetry_data) AND time_difference <= 1800;-- 1800 seconds = 30 minutes

  -- 14. the average temperature for each device during weekdays and weekends separately
SELECT device,
       CASE WHEN DATEPART(WEEKDAY, DATEADD(SECOND, ts, '19700101 00:00:00')) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       AVG(temp) AS average_temperature
FROM iot_telemetry_data
GROUP BY device, CASE WHEN DATEPART(WEEKDAY, DATEADD(SECOND, ts, '19700101 00:00:00')) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END;

-- 15 . the cumulative sum of temperature for each device, ordered by timestamp and limited to 10 records
WITH CumulativeTemperature AS (
    SELECT
        device,
        ts AS Timestamp,
        temp,
        LAG(temp, 1, 0) OVER (PARTITION BY device ORDER BY ts) AS prev_temp
    FROM
        iot_telemetry_data
),
RankedCumulativeTemperature AS (
    SELECT
        device,
        Timestamp,
        temp,
        SUM(prev_temp + temp) OVER (PARTITION BY device ORDER BY Timestamp) AS cumulative_temperature,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY Timestamp) AS rn
    FROM
        CumulativeTemperature
)
SELECT
    device,
    Timestamp,
    temp,
    cumulative_temperature
FROM
    RankedCumulativeTemperature
WHERE
    rn <= 10
ORDER BY
    device, Timestamp;