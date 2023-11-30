from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.sqlite_hook import SqliteHook
from datetime import datetime, timedelta
import clickhouse_driver

default_args = {
    'owner': 'pauloladapo'
}

with DAG(
    dag_id='fetch_clickhouse_and_insert_into_sqlite',
    description='Fetch data from ClickHouse and insert into SQLite',
    default_args=default_args,
    start_date=datetime(2023, 11, 30),  # Change as per your requirements
    schedule_interval=None,
    tags=['pipeline', 'sql']
) as dag:

    def fetch_clickhouse_data_and_insert_into_sqlite():
        try:
            # Establish ClickHouse connection
            clickhouse_conn = clickhouse_driver.connect(
                host='github.demo.trial.altinity.cloud',
                port=9440,
                database='default',
                user='demo',
                password='demo',
                secure=True  # Set to True if SSL encryption is required
            )

            # Fetch data from ClickHouse (example data)
            clickhouse_query = """
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
"""
            with clickhouse_conn.cursor() as cursor:
                cursor.execute(clickhouse_query)
                clickhouse_data = cursor.fetchall()

            # Establish SQLite connection using SqliteHook
            sqlite_hook = SqliteHook(sqlite_conn_id='my_sqlite_conn')
            sqlite_conn = sqlite_hook.get_conn()

            # Insert fetched data into SQLite table
            insert_query = """
                INSERT INTO metric_data 
                (month, sat_mean_trip_count, sat_mean_fare_per_trip,
                sat_mean_duration_per_trip, sun_mean_trip_count,
                sun_mean_fare_per_trip, sun_mean_duration_per_trip)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            with sqlite_conn:
                sqlite_cursor = sqlite_conn.cursor()
                for data in clickhouse_data:
                    sqlite_cursor.execute(insert_query, data)

        except Exception as e:
            # Log any error that occurred
            print(f"Error occurred: {str(e)}")
            raise

    insert_into_sqlite = PythonOperator(
        task_id='insert_into_sqlite',
        python_callable=fetch_clickhouse_data_and_insert_into_sqlite,
        dag=dag,
    )

insert_into_sqlite
