from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, \
    BigQueryCreateEmptyDatasetOperator

BUCKET = 'your_bucket_name'
DATASET_NAME = 'your_dataset_name'
REGIONS_TABLE_NAME = 'regions'
TRIPS_TABLE_NAME = 'trips'
STATION_INFO_TABLE_NAME = 'station_info'
STATION_STATUS_TABLE_NAME = 'station_status'
PROJECT_ID = 'your_project_id'
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'project_id': PROJECT_ID,
    'start_date': datetime.utcnow(),
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 0 * * *',
    'catchup': False,
}

with DAG(
        'bikepark_etl',
        default_args=DEFAULT_DAG_ARGS,
        description='This DAG provides some information about bikes and stations.',
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset",
        dataset_id=DATASET_NAME)

    load_regions_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_regions',
        bucket=BUCKET,
        source_objects=['bikepark/regions.csv'],
        destination_project_dataset_table=f'{DATASET_NAME}.{REGIONS_TABLE_NAME}',
        skip_leading_rows=1,
    )

    load_trips_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_trips',
        bucket=BUCKET,
        source_objects=['bikepark/trips*.csv'],
        destination_project_dataset_table=f'{DATASET_NAME}.{TRIPS_TABLE_NAME}',
        skip_leading_rows=1,
    )

    load_station_info = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_station_info',
        bucket=BUCKET,
        source_objects=['bikepark/station_info.json'],
        destination_project_dataset_table=f'{DATASET_NAME}.{STATION_INFO_TABLE_NAME}',
        source_format='NEWLINE_DELIMITED_JSON',
    )

    load_station_status = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_station_status',
        bucket=BUCKET,
        source_objects=['bikepark/station_status.json'],
        destination_project_dataset_table=f'{DATASET_NAME}.{STATION_STATUS_TABLE_NAME}',
        source_format='NEWLINE_DELIMITED_JSON',
    )

    create_bikes_docks_view = BigQueryCreateEmptyTableOperator(
        task_id='create_bikes_docks_view',
        dataset_id=DATASET_NAME,
        table_id='bikes_and_docks_status',
        view={
            'query': f"""
                        select
                            r.name,
                            r.region_id,
                            sum(ss.num_bikes_available) as total_available_bikes,
                            sum(ss.num_bikes_disabled) as total_disabled_bikes,
                            sum(ss.num_docks_available) as total_available_docks,
                            sum(ss.num_docks_disabled) as total_disabled_docks
                        from `{PROJECT_ID}.{DATASET_NAME}.{REGIONS_TABLE_NAME}` as r
                        inner join
                        `{PROJECT_ID}.{DATASET_NAME}.{STATION_INFO_TABLE_NAME}` as si
                        on si.region_id = r.region_id
                        inner join
                        `{PROJECT_ID}.{DATASET_NAME}.{STATION_STATUS_TABLE_NAME}` as ss
                        on si.station_id = ss.station_id
                        group by r.name, r.region_id
                    """,
            'useLegacySql': False,
        },
    )

    create_station_traffic_view = BigQueryCreateEmptyTableOperator(
        task_id='create_station_traffic_view',
        dataset_id=DATASET_NAME,
        table_id='station_traffic',
        view={
            'query': f"""
                        with temp_t_start as (
                            select
                                count(*) as total_start_trip,
                                t.start_station_name,
                                date(t.start_date) as start_date
                            from `{PROJECT_ID}.{DATASET_NAME}.{TRIPS_TABLE_NAME}` as t  
                            group by t.start_station_name, start_date          
                        ), temp_t_end as (
                            select
                                count(*) as total_end_trip,
                                t.end_station_name,
                                date(t.end_date) as end_date
                            from `{PROJECT_ID}.{DATASET_NAME}.{TRIPS_TABLE_NAME}` as t
                            group by t.end_station_name, end_date
                        )
                        select
                            s.start_station_name as station_name,
                            avg(s.total_start_trip) as trips_from_st_per_day,
                            avg(e.total_end_trip) as trips_in_st_per_day
                        from temp_t_start as s
                        inner join
                        temp_t_end as e
                        on s.start_station_name = e.end_station_name
                        group by station_name
                    """,
            'useLegacySql': False,
        },
    )

    create_top_bike_view = BigQueryCreateEmptyTableOperator(
        task_id='create_top_bike_view',
        dataset_id=DATASET_NAME,
        table_id='top_ten_bike',
        view={
            'query': f"""
                        select
                            bike_number
                        from (
                            select
                                bike_number,
                                count(*) total_num_of_trip
                            from `{PROJECT_ID}.{DATASET_NAME}.{TRIPS_TABLE_NAME}`
                            group by bike_number
                            order by total_num_of_trip desc
                            limit 10
                            )
                    """,
            'useLegacySql': False,
        },
    )

    create_bike_statistic_view = BigQueryCreateEmptyTableOperator(
        task_id='create_bike_statistic_view',
        dataset_id=DATASET_NAME,
        table_id='bike_statistic',
        view={
            'query': f"""                   
                        with temp_t as (
                            select 
                                bike_number,
                                member_gender,
                                subscriber_num,
                                ROW_NUMBER() OVER(PARTITION BY bike_number ORDER BY subscriber_num desc) AS num
                            from (
                                select
                                    bike_number,
                                    member_gender,
                                    count(*) as subscriber_num
                                from `{PROJECT_ID}.{DATASET_NAME}.{TRIPS_TABLE_NAME}`
                                where subscriber_type like 'Subscriber'
                                group by bike_number, member_gender
                            )
                        )
                        select
                            t.bike_number,
                            sum(t.duration_sec) as total_dur,
                            count(*) as total_trips,
                            temp_t.member_gender
                        from `{PROJECT_ID}.{DATASET_NAME}.{TRIPS_TABLE_NAME}` as t
                        inner join
                        temp_t 
                        on temp_t.bike_number = t.bike_number
                        where temp_t.num = 1
                        group by t.bike_number, temp_t.member_gender
                        order by t.bike_number
                        """,
            'useLegacySql': False,
        },
    )
    
    create_dataset >> [load_regions_csv, load_trips_csv, load_station_info, load_station_status] >> \
            create_bikes_docks_view >> create_station_traffic_view >> \
            create_top_bike_view >> create_bike_statistic_view
