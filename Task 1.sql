-- Selecting necessary metrics for Saturdays and Sundays and handling NULL values
SELECT 
    COALESCE(A.month, B.month) AS month,  -- Combining months from Saturday and Sunday subqueries
    A.sat_mean_trip_count,  -- Average number of trips on Saturdays
    A.sat_mean_fare_per_trip,  -- Average fare per trip on Saturdays
    A.sat_mean_duration_per_trip,  -- Average duration per trip on Saturdays
    B.sun_mean_trip_count,  -- Average number of trips on Sundays
    B.sun_mean_fare_per_trip,  -- Average fare per trip on Sundays
    B.sun_mean_duration_per_trip  -- Average duration per trip on Sundays
FROM
    -- Subquery for average metrics on Saturdays
    (SELECT 
        DATE_FORMAT(pickup_date, '%Y-%m') AS month,  -- Extracting month from pickup_date
        ROUND(AVG(trip_count), 1) AS sat_mean_trip_count,  -- Average trip count on Saturdays
        ROUND(AVG(total_amount), 1) AS sat_mean_fare_per_trip,  -- Average fare on Saturdays
        ROUND(AVG(duration_trip), 1) AS sat_mean_duration_per_trip  -- Average duration on Saturdays
    FROM
        -- Subquery to calculate metrics for Saturdays
        (SELECT 
            pickup_date,
            COUNT(*) AS trip_count,  -- Counting trips on Saturdays
            AVG(fare_amount) AS total_amount,  -- Calculating average fare on Saturdays
            AVG(TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime)) AS duration_trip  -- Calculating average duration on Saturdays
        FROM 
            tripdata
        WHERE 
            pickup_date BETWEEN STR_TO_DATE('2014-01-01', '%Y-%m-%d') AND STR_TO_DATE('2016-12-31', '%Y-%m-%d')  -- Filtering date range
            AND DAYOFWEEK(pickup_date) = 6  -- Filtering for Saturdays (6 is Saturday in MySQL)
        GROUP BY 
            pickup_date
        ) AS sat_metrics
    GROUP BY 
        DATE_FORMAT(pickup_date, '%Y-%m')  -- Grouping by month
    ORDER BY 
        DATE_FORMAT(pickup_date, '%Y-%m')  -- Sorting by month
    ) AS A  -- Alias for Saturday metrics
FULL OUTER JOIN
    -- Subquery for average metrics on Sundays
    (SELECT 
        DATE_FORMAT(pickup_date, '%Y-%m') AS month,  -- Extracting month from pickup_date
        ROUND(AVG(trip_count), 1) AS sun_mean_trip_count,  -- Average trip count on Sundays
        ROUND(AVG(total_amount), 1) AS sun_mean_fare_per_trip,  -- Average fare on Sundays
        ROUND(AVG(duration_trip), 1) AS sun_mean_duration_per_trip  -- Average duration on Sundays
    FROM
        -- Subquery to calculate metrics for Sundays
        (SELECT 
            pickup_date,
            COUNT(*) AS trip_count,  -- Counting trips on Sundays
            AVG(fare_amount) AS total_amount,  -- Calculating average fare on Sundays
            AVG(TIMESTAMPDIFF(MINUTE, pickup_datetime, dropoff_datetime)) AS duration_trip  -- Calculating average duration on Sundays
        FROM 
            tripdata
        WHERE 
            pickup_date BETWEEN STR_TO_DATE('2014-01-01', '%Y-%m-%d') AND STR_TO_DATE('2016-12-31', '%Y-%m-%d')  -- Filtering date range
            AND DAYOFWEEK(pickup_date) = 7  -- Filtering for Sundays (7 is Sunday in MySQL)
        GROUP BY 
            pickup_date
        ) AS sun_metrics
    GROUP BY 
        DATE_FORMAT(pickup_date, '%Y-%m')  -- Grouping by month
    ORDER BY 
        DATE_FORMAT(pickup_date, '%Y-%m')  -- Sorting by month
    ) AS B  -- Alias for Sunday metrics
ON
    A.month = B.month;  -- Joining Saturday and Sunday metrics on month
