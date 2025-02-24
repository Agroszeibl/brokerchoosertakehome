'''
Class to handle db operations

Loads files using dask batch loads from predetermined csv files

Creates staging layer with raw data

Data cleanup in int layer

Finally fuzzy join and final table in analytics layer
'''

from time import sleep
from pathlib import Path

import duckdb
from loguru import logger
import dask.dataframe as dd

class DbHandler:

    def __init__(self, source_folder, db_path, check_interval=60):
        self.db_path = db_path
        self.source_folder = source_folder
        self.check_interval = check_interval

    def run_query(self, query):
        '''
        Base method to connect to db and run queries. Everything else needs to built on top
        No return so should be used for stuff like creating tables and databases
        Or inserting data etc

        Args:   
            query (str): SQL query to be executed

        Returns:
            None
        '''
        try:
            conn = duckdb.connect(self.db_path)
            conn.execute(query)
            conn.close()

        except duckdb.Error as e:
            logger.error(f"DuckDB Error: {e}")
            logger.error(f'Query: {query}')

        except Exception as e:
            logger.error(f"Unexpected Error: {e}")
            logger.error(f'Query: {query}')

        finally:
            if conn:
                conn.close()

    def fetch_df(self, query):
        '''
        Base method to connect to db and fetch queries. 
        Every pull operation needs to be built on top.

        Args:
            query (str): SQL query to be executed

        Returns:
            pd.DataFrame: Dataframe with the results of query
        '''
        conn = duckdb.connect(self.db_path)
        result = conn.execute(query).fetchdf()
        conn.close()
        return result

    def create_db(self):
        '''
        Method to create db.

        Args:
            None

        Returns:
            None
        '''
        query = """
        SELECT 1;
        """
        try:
            self.run_query(query)
            logger.info('DB created successfully')
        except Exception as e:
            logger.error(f'Error creating db: {e}')

    '''
    These methods will use to generate staging layer within db
    Note: I will follow ELT practice, so I will load these full as VARCHAR and then cleanup in int layer
    '''

    def broker_data(self):
        '''
        Method to load broker data into staging layer

        Args:
            None

        Returns:
            None
        '''
        query = """
        CREATE OR REPLACE TABLE staging.broker_data (
            timestamp VARCHAR
            , country_residency VARCHAR
            , ip_country VARCHAR
            , important_score VARCHAR
            , batched_at TIMESTAMP
        );
        """
        self.run_query(query)

    def brokerchooser_conversions(self):
        '''
        Method to load brokerchooser conversions data into staging layer

        Args:
            None

        Returns:
            None
        '''
        query = """
        CREATE OR REPLACE TABLE staging.brokerchooser_conversions (
            id VARCHAR
            , session_id VARCHAR
            , country_name VARCHAR
            , is_mobile VARCHAR
            , ui_element VARCHAR
            , created_at VARCHAR
            , measurement_category VARCHAR
            , batched_at TIMESTAMP
        );
        """
        self.run_query(query)

    def page_category_mapping(self):
        '''
        Method to load page category mapping data into staging layer

        Args:
            None

        Returns:
            None
        '''
        query = """
        CREATE OR REPLACE TABLE staging.page_category_mapping (
            page_category VARCHAR
            , measurement_category VARCHAR
            , batched_at TIMESTAMP
        );
        """
        self.run_query(query)

    def register_pandas(self, df_name, df):
        '''
        Register a pandas df as a temp table in db

        Args:
            df (pd.DataFrame): Dataframe to be registered

        Returns:
            None        
        '''
        conn = duckdb.connect(self.db_path)
        conn.register(df_name, df)
        conn.close()

    def insert(self, table_ref, df):
        '''
        Method to insert df into db
        Note: needs its own conn setup otherwise duckdb doesn't persist the df for loading

        Args:
            df (pd.DataFrame): Dataframe to be inserted
            table_name (str): Name of table to insert into

        Returns:
            None
        '''
        conn = duckdb.connect(self.db_path)   
        df = df   
        query = f'INSERT INTO {table_ref} SELECT *, CURRENT_TIMESTAMP AS batched_at FROM df'
        conn.execute(query)

    def load_db(self, csv_path, table_ref, batch_size="1MB", delimiter=','):
        '''
        Method to load data into given table

        Note: I wasn't sure if using duckdb native csv load is allowed, so used dask for batches
        Native duckdb might be more efficient though

        Args:
            None

        Returns:
            None
        '''
    
        try:
            df = dd.read_csv(csv_path, blocksize=batch_size, dtype=str, delimiter=delimiter)

            for partition in df.to_delayed():
                partition_df = partition.compute()
                self.insert(table_ref, partition_df)

            logger.info(f"Loaded {csv_path} into {table_ref} in DuckDB.")

        except Exception as e:
            logger.error(f"Error loading {csv_path} into DuckDB: {e}")

    '''
    These methods are to create our int layer
    Also to query the data from our staging layer, cleanup, and append to int layer
    '''
        
    def int_broker_data(self):
        '''
        Method to load broker data into staging layer

        Args:
            None

        Returns:
            None
        '''
        query = """
        CREATE OR REPLACE TABLE int.broker_data (
            id VARCHAR PRIMARY KEY
            , timestamp DATETIME
            , country_residency VARCHAR
            , ip_country VARCHAR
            , important_score INT
            , batched_at TIMESTAMP
        );
        """
        self.run_query(query)

    def load_int_broker_data(self):
        '''
        Method for incremental updates

        Note that insert should work as duplicates will be replaced
        As per table definition

        Args:
            None

        Returns:
            None
        '''

        query = """
            INSERT INTO int.broker_data (
                id
                , timestamp
                , country_residency
                , ip_country
                , important_score
                , batched_at
            )

            SELECT 
                CONCAT(timestamp, '/', ip_country) AS id
                , TO_TIMESTAMP(timestamp::INT) AS timestamp
                , UPPER(TRIM(country_residency)) AS country_residency
                , UPPER(TRIM(ip_country)) AS ip_country
                , IF(important_score = '#REF!', 0, important_score::INT) AS important_score
                , CURRENT_TIMESTAMP AS batched_at
            FROM staging.broker_data
            WHERE batched_at > ( SELECT COALESCE(MAX(batched_at), '1970-01-01') FROM int.page_category_mapping )

            --qualify to deduplicate... Kinda costly. Accepting as using batched at filtering
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC) = 1;
            """
        self.run_query(query)

    def int_brokerchooser_conversions(self):
        '''
        Method to load brokerchooser conversions data into staging layer

        Args:
            None

        Returns:
            None
        '''
        query = """
        CREATE OR REPLACE TABLE int.brokerchooser_conversions (
            id INT PRIMARY KEY
            , session_id INT
            , country_name VARCHAR
            , is_mobile BOOLEAN
            , ui_element VARCHAR
            , created_at TIMESTAMP
            , measurement_category VARCHAR
            , batched_at TIMESTAMP
        );
        """
        self.run_query(query)

    def load_int_brokerchooser_conversions(self):
        '''
        Method for incremental updates of brokerchooser conversions data.

        Args:
            None

        Returns:
            None
        '''

        query = """
            INSERT INTO int.brokerchooser_conversions (
                id
                , session_id
                , country_name
                , is_mobile
                , ui_element
                , created_at
                , measurement_category
                , batched_at
            )
            SELECT 
                id::INT AS id
                , session_id::INT AS session_id
                , UPPER(TRIM(country_name)) AS country_name
                , is_mobile::BOOLEAN AS is_mobile
                , ui_element
                , created_at
                , measurement_category
                , CURRENT_TIMESTAMP AS batched_at
            FROM staging.brokerchooser_conversions
            WHERE batched_at > ( SELECT COALESCE(MAX(batched_at), '1970-01-01') FROM int.brokerchooser_conversions )
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1;
        """
        self.run_query(query)

    def int_page_category_mapping(self):
        '''
        Method to load page category mapping data into staging layer

        Args:
            None

        Returns:
            None
        '''
        query = """
        CREATE OR REPLACE TABLE int.page_category_mapping (
            page_category VARCHAR PRIMARY KEY
            , measurement_category VARCHAR
            , batched_at TIMESTAMP
        );
        """
        self.run_query(query)

    def load_int_page_category_mapping(self):
        '''
        Method for incremental updates of page category mapping data.

        Args:
            None

        Returns:
            None
        '''

        query = """
            INSERT INTO int.page_category_mapping (
                page_category
                , measurement_category
                , batched_at
            )
            SELECT 
                page_category
                , measurement_category
                , CURRENT_TIMESTAMP AS batched_at
            FROM staging.page_category_mapping
            WHERE batched_at > ( SELECT COALESCE(MAX(batched_at), '1970-01-01') FROM int.page_category_mapping );
        """
        self.run_query(query)

    def analytics_brokerchooser(self):
        '''
        Creates analytics layer to be used for EDA
        '''

        query = """
        CREATE OR REPLACE TABLE analytics.brokerchooser (
                conversion_id INT PRIMARY KEY
                , session_id INT
                , session_action_counter INT
                , last_action_by_session_count INT
                , is_last_action_by_session BOOLEAN
                , conversions_by_session INT
                , conversion_country VARCHAR
                , is_mobile BOOLEAN
                , ui_element VARCHAR
                , created_at TIMESTAMP
                , measurement_category VARCHAR

                , page_category VARCHAR

                , broker_id VARCHAR
                , conversion_mapped_count INT
                , is_several_conversion BOOLEAN
                , broker_timestamp TIMESTAMP
                , broker_country_residency VARCHAR
                , broker_ip_country VARCHAR
                , important_score INT
                , important_score_bucket VARCHAR

                , conversion_seconds INT
                , is_converted BOOLEAN

                , batched_at TIMESTAMP
        )

        """
        self.run_query(query)

    def load_analytics_brokerchooser(self):
        '''
        Loads analytics layer

        SQL here is a bit more complicated
        As need to create fuzzy matching join
        '''

        query = """
        INSERT INTO analytics.brokerchooser (
                conversion_id
                , session_id
                , session_action_counter
                , last_action_by_session_count
                , is_last_action_by_session
                , conversions_by_session
                , conversion_country
                , is_mobile
                , ui_element
                , created_at
                , measurement_category
                , page_category
                , broker_id
                , conversion_mapped_count
                , is_several_conversion
                , broker_timestamp
                , broker_country_residency
                , broker_ip_country
                , important_score
                , important_score_bucket
                , conversion_seconds
                , is_converted
                , batched_at
            )

        WITH base AS (
        SELECT
            brokerchooser_conversions.id AS conversion_id
            , brokerchooser_conversions.session_id
            , brokerchooser_conversions.country_name AS conversion_country
            , brokerchooser_conversions.is_mobile
            , brokerchooser_conversions.ui_element
            , brokerchooser_conversions.created_at
            , brokerchooser_conversions.measurement_category

            , page_category_mapping.page_category

            , broker_data.id AS broker_id
            , broker_data.timestamp AS broker_timestamp
            , broker_data.country_residency AS broker_country_residency
            , broker_data.ip_country AS broker_ip_country
            , broker_data.important_score AS important_score

            --metrics to be used for analysis

            --null handling for conversion seconds. Needed for cleanup in final CTE
            , CASE
                WHEN broker_data.timestamp IS NOT NULL THEN ABS(DATEDIFF('SECOND', broker_data.timestamp, brokerchooser_conversions.created_at))
                ELSE 0 
            END AS conversion_seconds
            , IF(broker_timestamp IS NOT NULL, TRUE, FALSE) AS is_converted

            , CURRENT_TIMESTAMP AS batched_at
        FROM int.brokerchooser_conversions
        LEFT JOIN int.page_category_mapping ON page_category_mapping.measurement_category = brokerchooser_conversions.measurement_category
        
        LEFT JOIN int.broker_data ON 
            broker_data.timestamp > brokerchooser_conversions.created_at
            --Assuming that if the time diff is larger than 2 hours, then the conversion didn't happen because of the website action
            AND ABS(DATEDIFF('MINUTE', broker_data.timestamp, brokerchooser_conversions.created_at)) <= 120
            --Finally checking if either the residency or the IP country matches
            AND (
                broker_data.country_residency = brokerchooser_conversions.country_name
                OR broker_data.ip_country = brokerchooser_conversions.country_name
                )
        
        --finally need the where statement for incrementality
        WHERE brokerchooser_conversions.batched_at > ( SELECT COALESCE(MAX(batched_at), '1970-01-01') FROM analytics.brokerchooser )
        )

        , base2 AS (
            SELECT * FROM base
            --qualify to only attribute conversion to first matching timestamp
            QUALIFY ROW_NUMBER() OVER (PARTITION BY conversion_id ORDER BY conversion_seconds ASC) = 1
        )

        , base3 AS (--using final CTE to make sure I only attribute one conversion to one id in the conversion table
            SELECT
                *

                , MIN(conversion_seconds) OVER (PARTITION BY broker_id) AS min_converted_in
            FROM base2
        )

        , penultimate AS (--gonna test for several conversions in final select after final cte
            SELECT
                conversion_id
                , session_id
                , conversion_country
                , is_mobile
                , ui_element
                , created_at
                , measurement_category
                , page_category

                , IF(min_converted_in != conversion_seconds, NULL, broker_id) AS broker_id
                , IF(min_converted_in != conversion_seconds, NULL, broker_timestamp) AS broker_timestamp
                , IF(min_converted_in != conversion_seconds, NULL, broker_country_residency) AS broker_country_residency
                , IF(min_converted_in != conversion_seconds, NULL, broker_ip_country) AS broker_ip_country
                , IF(min_converted_in != conversion_seconds, NULL, important_score) AS important_score
                , IF(min_converted_in != conversion_seconds, NULL, conversion_seconds) AS conversion_seconds
                , IF(min_converted_in != conversion_seconds, NULL, is_converted) AS is_converted

                , batched_at
            FROM base3
        )

        , final AS (
            SELECT
                conversion_id
                , session_id
                , ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at ASC) AS session_action_counter
                , COUNT(DISTINCT broker_id) OVER (PARTITION BY session_id) AS conversions_by_session
                , conversion_country
                , is_mobile
                , ui_element
                , created_at
                , measurement_category
                , page_category

                , broker_id
                , CASE
                    WHEN broker_id IS NOT NULL THEN COUNT(DISTINCT conversion_id) OVER (PARTITION BY broker_id) 
                    ELSE 0
                END AS conversion_mapped_count
                , IF(conversion_mapped_count > 1, TRUE, FALSE) AS is_several_conversion
                , broker_timestamp
                , broker_country_residency
                , broker_ip_country
                , important_score
                , CASE
                    WHEN important_score IS NULL THEN '0 - No Score'
                    WHEN important_score < 10 THEN '1 - Below 10'
                    WHEN important_score < 25 THEN '2 - Below 25'
                    WHEN important_score < 50 THEN '3 - Below 50'
                    WHEN important_score < 75 THEN '4 - Below 75'
                    WHEN important_score < 90 THEN '5 - Below 90'
                    ELSE '6 - Above 90'
                END AS important_score_bucket
                , conversion_seconds
                , is_converted

                , batched_at
            FROM penultimate
        )

        SELECT
            conversion_id
            , session_id
            , session_action_counter
            , MAX(session_action_counter) OVER (PARTITION BY session_id) AS last_action_by_session_count
            , IF(session_action_counter = last_action_by_session_count, TRUE, FALSE) AS is_last_action_by_session
            , conversions_by_session
            , conversion_country
            , is_mobile
            , ui_element
            , created_at
            , measurement_category
            , page_category
            , broker_id
            , conversion_mapped_count
            , is_several_conversion
            , broker_timestamp
            , broker_country_residency
            , broker_ip_country
            , important_score
            , important_score_bucket
            , conversion_seconds
            , is_converted
            , batched_at
        FROM final
        
        ;
        """

        '''
        Note: after all this logic, 86 conversions are still mapped to several id-s
        Ignoring as insignificant, but can be queried like this:
            SELECT 
                * 
            FROM analytics.brokerchooser
            WHERE 
                conversion_mapped_count > 1
        '''

        self.run_query(query)

    def create_tables(self):
        '''
        Creates all tables we need

        Limitation: create or replace statements
        --> used these for quick iteration
        '''

        self.broker_data()
        self.brokerchooser_conversions()
        self.page_category_mapping()

        self.int_broker_data()
        self.int_brokerchooser_conversions()
        self.int_page_category_mapping()

        self.analytics_brokerchooser()

    def load_staging(self):
        '''
        Loads staging layer

        Using dask batch loads
        '''

        BASE_DIR = Path(__file__).resolve().parent.parent

        broker_data_path = BASE_DIR/'db'/'sources'/'broker_data.csv'
        brokerchooser_conversions_path = BASE_DIR /'db'/'sources'/'brokerchooser_conversions.csv'
        page_category_mapping_path = BASE_DIR/'db'/'sources'/'page_category_mapping.csv'

        self.load_db(
            csv_path = broker_data_path,
            table_ref = 'staging.broker_data'
        )

        self.load_db(
            csv_path = brokerchooser_conversions_path,
            table_ref = 'staging.brokerchooser_conversions',
            delimiter=';'
        )

        self.load_db(
            csv_path = page_category_mapping_path,
            table_ref = 'staging.page_category_mapping',
            delimiter=';'
        )

    def load_int(self):
        '''
        Loads intermediate layer with data cleanup
        '''

        self.load_int_broker_data()
        self.load_int_brokerchooser_conversions()
        self.load_int_page_category_mapping()

    def load_analytics(self):
        '''
        Method to update analytics layer
        ... Currently only one method, but can scale into more
        Creating to have a std approach
        
        '''
        self.load_analytics_brokerchooser()

    def create_schemas(self):
        '''
        Func to create schemas for our db

        Args:
            None

        Returns:
            None
        '''
        staging_query = 'CREATE SCHEMA IF NOT EXISTS staging;'
        int_query = 'CREATE SCHEMA IF NOT EXISTS int;'
        analytics_query = 'CREATE SCHEMA IF NOT EXISTS analytics;'

        self.run_query(staging_query)
        self.run_query(int_query)
        self.run_query(analytics_query)

    def run_db(self, create_tables=False):
        '''
        Runs db load and create operations

        Args:
            create_tables: BOOLEAN: whether to create replace tables
            --> set as false if just wanna run an incremental update
        '''
        if create_tables:
            self.create_tables()

        self.load_staging()
        self.load_int()
        self.load_analytics()

    def db_dag(self, sleep_time=24*60*60):
        '''
        Continuous loop to update our tables based on schedule

        ... essentially a fake cron job
        '''

        while True:
            self.run_db()
            sleep(sleep_time)
