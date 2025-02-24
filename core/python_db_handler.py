'''
Class to handle pipeline entirely in Python

Same principles as the SQL handler, but with Python
'''

import os
from pathlib import Path

import pandas as pd
import dask.dataframe as dd
from loguru import logger
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np

class PythonDbHandler:

    BASE_DIR = Path(__file__).resolve().parent.parent

    def __init__(self):
        '''
        Init, mostly used to store folder paths
        '''

        self.staging_path = self.BASE_DIR/'python_db'/'staging'
        self.int_path = self.BASE_DIR/'python_db'/'int'
        self.analytics_path = self.BASE_DIR/'python_db'/'analytics'
        self.analytics_cleaned_path = self.BASE_DIR/'python_db'/'analytics_cleaned'

    def batch_read(self):
        pass

    def ingest_csv(self, csv_path, parquet_dir, batch_size="1MB", delimiter=','):
        """
        Method to load CSV data into a Parquet dataset (directory) in batches.

        - If the dataset directory does not exist, it will be created.
        - If the dataset exists, new data will be appended as a new Parquet file.

        Args:
            csv_path (str): Path to the CSV file.
            parquet_dir (str): Path to the Parquet dataset directory.
            batch_size (str): Dask block size for reading.
            delimiter (str): CSV delimiter.

        Returns:
            None
        """

        try:
            df = dd.read_csv(csv_path, blocksize=batch_size, dtype=str, delimiter=delimiter)

            if not os.path.exists(parquet_dir):
                os.makedirs(parquet_dir, exist_ok=True)
                logger.info(f"Created dataset directory: {parquet_dir}")

            for partition in df.to_delayed():
                partition_df = partition.compute()

                table = pa.Table.from_pandas(partition_df)

                pq.write_to_dataset(table, root_path=parquet_dir, filesystem=None)

            logger.info(f"Successfully loaded {csv_path} into {parquet_dir} (appended as a new file).")

        except Exception as e:
            logger.error(f"Error loading {csv_path} into Parquet dataset: {e}")

    def create_parquet_dataset(self, folder_path, dataset_name, columns, dtypes):
        """
        Creates an empty Parquet dataset (directory) in the specified folder with given columns and data types.

        Args:
            folder_path (str): Directory where the Parquet dataset will be stored.
            dataset_name (str): Name of the Parquet dataset (will be a directory).
            columns (list): List of column names.
            dtypes (dict): Dictionary mapping column names to their data types.

        Returns:
            None
        """

        dataset_path = os.path.join(folder_path, dataset_name)

        if os.path.exists(dataset_path) and not os.path.isdir(dataset_path):
            try:
                os.remove(dataset_path)
                logger.info(f"Deleted existing file: {dataset_path} (converted to dataset directory)")
            except Exception as e:
                logger.error(f"Error deleting existing file: {e}")
                return

        os.makedirs(dataset_path, exist_ok=True)

        df = pd.DataFrame(columns=columns).astype(dtypes)

        try:
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, root_path=dataset_path, filesystem=None)
            logger.info(f"Created new Parquet dataset: {dataset_path}")

        except Exception as e:
            logger.error(f"Error creating Parquet dataset: {e}")
    
    def staging_broker_data(self):
        '''
        Method to create staging broker data parquet file
        
        Args:
            None

        Returns:
            None
        '''
        file_name = 'staging_broker_data'
        columns = ['timestamp', 'country_residency', 'ip_country', 'important_score']
        dtypes = {'timestamp': 'string', 'country_residency': 'string', 'ip_country': 'string', 'important_score': 'string'}

        self.create_parquet_dataset(
            self.staging_path,
            file_name,
            columns,
            dtypes
        )

    def load_staging_broker_data(self):
        '''
        Loads staging broker data parquet
        Using our parquet loader method
        '''
        self.ingest_csv(
            '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/sources/broker_data.csv',
            '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/staging/staging_broker_data'
        )

    def staging_brokerchooser_conversions(self):
        '''
        Method to create staging brokerchooser conversions parquet file
        
        Args:
            None

        Returns:
            None
        '''
        file_name = 'staging_brokerchooser_conversions'
        columns = ['id', 'session_id', 'country_name', 'is_mobile', 'ui_element', 'created_at', 'measurement_category']
        dtypes = {
            'id': 'string', 
            'session_id': 'string', 
            'country_name': 'string', 
            'is_mobile': 'string', 
            'ui_element': 'string', 
            'created_at': 'string', 
            'measurement_category': 'string'}

        self.create_parquet_dataset(
            self.staging_path,
            file_name,
            columns,
            dtypes
        )

    def load_staging_brokerchooser_conversions(self):
        '''
        Loads staging brokerchooser conversions parquet
        Using our parquet loader method
        '''
        self.ingest_csv(
            '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/sources/brokerchooser_conversions.csv',
            '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/staging/staging_brokerchooser_conversions',
            delimiter=';'
        )

    def staging_page_category_mapping(self):
        '''
        Method to create staging page category mapping file
        
        Args:
            None

        Returns:
            None
        '''
        file_name = 'staging_page_category_mapping'
        columns = ['page_category', 'measurement_category']
        dtypes = {'page_category': 'string', 'measurement_category': 'string'}

        self.create_parquet_dataset(
            self.staging_path,
            file_name,
            columns,
            dtypes
        )

    def load_staging_page_category_mapping(self):
        '''
        Loads staging page category mapping parquet
        Using our parquet loader method
        '''
        self.ingest_csv(
            '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/sources/page_category_mapping.csv',
            '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/staging/staging_page_category_mapping',
            delimiter=';'
        )

    def int_broker_data(self):
        '''
        Method to create int broker data parquet file
        
        Args:
            None

        Returns:
            None
        '''
        file_name = 'int_broker_data'
        columns = ['id', 'timestamp', 'country_residency', 'ip_country', 'important_score']
        dtypes = {'id': 'string', 'timestamp': 'datetime64[ns]', 'country_residency': 'string', 'ip_country': 'string', 'important_score': 'int64'}

        self.create_parquet_dataset(
            self.int_path,
            file_name,
            columns,
            dtypes
        )

    def load_int_broker_data(self):
        '''
        Method to load our int broker data table

        We're cleaning up dtypes and errors here
        '''

        staging_parquet = '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/staging/staging_broker_data'
        int_parquet = '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/int/int_broker_data'

        df = dd.read_parquet(staging_parquet, engine="pyarrow")
        delayed_partitions = df.to_delayed()

        for partition in delayed_partitions:
            partition_df = partition.compute()

            partition_df['id'] = partition_df['timestamp'] + '/' + partition_df['ip_country']
            partition_df["timestamp"] = dd.to_datetime(partition_df["timestamp"].astype(int), unit="s")
            partition_df["country_residency"] = partition_df["country_residency"].str.strip().str.upper()
            partition_df["ip_country"] = partition_df["ip_country"].str.strip().str.upper()
            partition_df["important_score"] = partition_df["important_score"].mask(partition_df["important_score"] == "#REF!", "0")
            partition_df["important_score"] = partition_df["important_score"].astype(int)

            table = pa.Table.from_pandas(partition_df)
            pq.write_to_dataset(table, root_path=int_parquet, filesystem=None)

    def int_brokerchooser_conversions(self):
        '''
        Method to create int brokerchooser conversions parquet file
        
        Args:
            None

        Returns:
            None
        '''
        file_name = 'int_brokerchooser_conversions'
        columns = ['id', 'session_id', 'country_name', 'is_mobile', 'ui_element', 'created_at', 'measurement_category']
        dtypes = {
            'id': 'int64', 
            'session_id': 'int64', 
            'country_name': 'string', 
            'is_mobile': 'boolean', 
            'ui_element': 'string', 
            'created_at': 'datetime64[ns]', 
            'measurement_category': 'string'}

        self.create_parquet_dataset(
            self.int_path,
            file_name,
            columns,
            dtypes
        )

    def load_int_brokerchooser_conversions(self):
        '''
        Method to load our int brokerchooser conversions table

        We're cleaning up dtypes and errors here
        '''

        staging_parquet = '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/staging/staging_brokerchooser_conversions'
        int_parquet = '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/int/int_brokerchooser_conversions'

        df = dd.read_parquet(staging_parquet, engine="pyarrow")
        delayed_partitions = df.to_delayed()

        for partition in delayed_partitions:
            partition_df = partition.compute()

            partition_df["id"] = partition_df["id"].astype(int)
            partition_df["session_id"] = partition_df["session_id"].astype(int)
            partition_df["country_name"] = partition_df["country_name"].str.strip().str.upper()
            partition_df["is_mobile"] = partition_df["is_mobile"].astype(bool)

            # deduplicating id-s
            partition_df = partition_df.sort_values("created_at").groupby("id").tail(1)

            table = pa.Table.from_pandas(partition_df)
            pq.write_to_dataset(table, root_path=int_parquet, filesystem=None)

    def int_page_category_mapping(self):
        '''
        Method to create int page category mapping file
        
        Args:
            None

        Returns:
            None
        '''
        file_name = 'int_page_category_mapping'
        columns = ['page_category', 'measurement_category']
        dtypes = {'page_category': 'string', 'measurement_category': 'string'}

        self.create_parquet_dataset(
            self.int_path,
            file_name,
            columns,
            dtypes
        )

    def load_int_page_category_mapping(self):
        '''
        Method to load our int page category mapping table

        We're cleaning up dtypes and errors here
        '''

        staging_parquet = '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/staging/staging_page_category_mapping'
        int_parquet = '/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/int/int_page_category_mapping'

        df = dd.read_parquet(staging_parquet, engine="pyarrow")
        delayed_partitions = df.to_delayed()

        for partition in delayed_partitions:
            partition_df = partition.compute()

            table = pa.Table.from_pandas(partition_df)
            pq.write_to_dataset(table, root_path=int_parquet, filesystem=None)

    def analytics_brokerchooser(self):
        '''
        Method to create our analytics brokerchooser model

        This is where we do the attribution, and the only table that should be used for analysis
        '''
        file_name = 'analytics_brokerchooser'
        columns = [
            "conversion_id",
            "session_id",
            "session_action_counter",
            "last_action_by_session_count",
            "conversions_by_session",
            "conversion_country",
            "is_mobile",
            "ui_element",
            "created_at",
            "measurement_category",
            "page_category",
            "broker_id",
            "broker_timestamp",
            "broker_country_residency",
            "broker_ip_country",
            "important_score",
            "important_score_bucket",
            "conversion_seconds",
            "is_converted",
        ]

        dtypes = {
            "conversion_id": "int64",
            "session_id": "int64",
            "session_action_counter": "int64",
            "last_action_by_session_count": "int64",
            "conversions_by_session": "int64",
            "conversion_country": "string",
            "is_mobile": "boolean",
            "ui_element": "string",
            "created_at": "datetime64[ns]",
            "measurement_category": "string",
            "page_category": "string",
            "broker_id": "string",
            "broker_timestamp": "datetime64[ns]",
            "broker_country_residency": "string",
            "broker_ip_country": "string",
            "important_score": "int64",
            "important_score_bucket": "string",
            "conversion_seconds": "int64",
            "is_converted": "boolean",
        }

        self.create_parquet_dataset(
            self.analytics_path,
            file_name,
            columns,
            dtypes
        )

    def analytics_brokerchooser_cleaned(self):
        '''
        Method to create our analytics brokerchooser cleaned model

        This is where we do the attribution, and the only table that should be used for analysis
        '''
        file_name = 'analytics_brokerchooser_cleaned'
        columns = [
            "conversion_id",
            "session_id",
            "session_action_counter",
            "last_action_by_session_count",
            "conversions_by_session",
            "conversion_country",
            "is_mobile",
            "ui_element",
            "created_at",
            "measurement_category",
            "page_category",
            "broker_id",
            "broker_timestamp",
            "broker_country_residency",
            "broker_ip_country",
            "important_score",
            "important_score_bucket",
            "conversion_seconds",
            "is_converted",
        ]

        dtypes = {
            "conversion_id": "int64",
            "session_id": "int64",
            "session_action_counter": "int64",
            "last_action_by_session_count": "int64",
            "conversions_by_session": "int64",
            "conversion_country": "string",
            "is_mobile": "boolean",
            "ui_element": "string",
            "created_at": "datetime64[ns]",
            "measurement_category": "string",
            "page_category": "string",
            "broker_id": "string",
            "broker_timestamp": "datetime64[ns]",
            "broker_country_residency": "string",
            "broker_ip_country": "string",
            "important_score": "int64",
            "important_score_bucket": "string",
            "conversion_seconds": "int64",
            "is_converted": "boolean",
        }

        self.create_parquet_dataset(
            self.analytics_cleaned_path,
            file_name,
            columns,
            dtypes
        )

    def load_analytics_brokerchooser(self):
        '''
        Method to load data into our analytics brokerchooser parquet

        This is a bit tricky due to the fuzzy matching logic
        '''

        # preparing col names for join
        logger.info("Starting col names cleanup.")
        broker_data = dd.read_parquet("/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/int/int_broker_data/")
        broker_data['broker_id'] = broker_data['id']
        broker_data.drop(columns=['broker_id'])

        broker_data['broker_timestamp'] = broker_data['timestamp']
        broker_data.drop(columns=['broker_timestamp'])

        broker_data['broker_country_residency'] = broker_data['country_residency']
        broker_data.drop(columns=['broker_country_residency'])

        broker_data['broker_ip_country'] = broker_data['ip_country']
        broker_data.drop(columns=['broker_ip_country'])


        conversions = dd.read_parquet("/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/int/int_brokerchooser_conversions/")
        conversions['conversion_id'] = conversions['id']
        conversions.drop(columns=['conversion_id'])

        conversions['conversion_country'] = conversions['country_name']
        conversions.drop(columns=['conversion_country'])

        page_category_mapping = dd.read_parquet("/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/int/int_page_category_mapping/")

        logger.info('col names cleanup finished')

        logger.critical('starting partition computes')
        for partition in conversions.to_delayed():  
            logger.critical('starting to compute new partition')
            partition_df = partition.compute()

            broker_data_pd = broker_data.compute()
            page_category_mapping_pd = page_category_mapping.compute()
            logger.critical('partition computed')

            # 1) We're performing our base joins
            logger.info('starting base joins')
            partition_df = partition_df.merge(
                page_category_mapping_pd, 
                how="left", 
                on="measurement_category"
            )

            partition_df["created_at"] = pd.to_datetime(partition_df["created_at"], errors="coerce")
            broker_data_pd["broker_timestamp"] = pd.to_datetime(broker_data_pd["broker_timestamp"], errors="coerce")
            partition_df = partition_df.sort_values("created_at")
            broker_data_pd = broker_data_pd.sort_values("broker_timestamp")
            merged_df = pd.merge_asof(
                partition_df, broker_data_pd,
                left_on="created_at", right_on="broker_timestamp",
                direction="nearest"
            )
            logger.info('fuzzy join finished')

            # 3) Now we need to create the qualify row number logic, to remove rows we created via above matching
            merged_df["conversion_seconds"] = merged_df.apply(
                lambda row: abs((row["broker_timestamp"] - row["created_at"]).total_seconds())
                if pd.notnull(row["broker_timestamp"])
                else 0,
                axis=1,
            )

            merged_df["is_converted"] = merged_df["broker_timestamp"].notnull()

            logger.info('conversion seconds and is converted finished')

            # 4) The rest is straightforward, we'll create the remaining columns

            # 4a) important score buckets
            def score_bucket(score):
                if pd.isnull(score):
                    return "0 - No Score"
                elif score < 10:
                    return "1 - Below 10"
                elif score < 25:
                    return "2 - Below 25"
                elif score < 50:
                    return "3 - Below 50"
                elif score < 75:
                    return "4 - Below 75"
                elif score < 90:
                    return "5 - Below 90"
                else:
                    return "6 - Above 90"

            merged_df["important_score_bucket"] = merged_df["important_score"].apply(score_bucket)

            # 4b) conversions by session
            merged_df["conversions_by_session"] = merged_df.groupby("session_id")["broker_id"].nunique()

            # 4c) session action counter
            merged_df["session_action_counter"] = merged_df.groupby("session_id")["created_at"].rank(method="first", ascending=True)
            merged_df["last_action_by_session_count"] = merged_df.groupby("session_id")["session_action_counter"].transform("max")

            logger.info('score buckets and session counters finished')

            # 5) We'll reorder our columns to the expected order
            columns = [
                "conversion_id",
                "session_id",
                "session_action_counter",
                "last_action_by_session_count",
                "conversions_by_session",
                "conversion_country",
                "is_mobile",
                "ui_element",
                "created_at",
                "measurement_category",
                "page_category",
                "broker_id",
                "broker_timestamp",
                "broker_country_residency",
                "broker_ip_country",
                "important_score",
                "important_score_bucket",
                "conversion_seconds",
                "is_converted",
            ]
            merged_df = merged_df[columns]

            table = pa.Table.from_pandas(merged_df)

            analytics_path = self.BASE_DIR/'python_db'/'analytics'/'analytics_brokerchooser'
            pq.write_to_dataset(table, root_path=analytics_path, filesystem=None)
            logger.info(f"Processed batch and saved to {analytics_path}")

        logger.info("Completed processing all batches.")

    def analytics_brokerchooser_cleanup(self):
        '''
        Need one more method to clean up the attribution logic in the analytics brokerchooser table

        Gonna use dask by partition... Probably results in some attribution error still, but keeping it for now to optimize

        Note: Can I setup manual partitioning logic for the parquet dataset to remove the attribution error?
        '''
        analytics_data = dd.read_parquet("/Users/adamgroszeibl/Documents/airflow_brokerchooser/analytics_pipeline/python_db/analytics/analytics_brokerchooser/")

        for partition in analytics_data.to_delayed():

            partition_df = partition.compute()
            partition_df["min_conversion_seconds"] = partition_df.groupby("broker_id")["conversion_seconds"].transform("min")

            # essentially just removing duplicate attributions if there are several broker id-s attributed to an action
            partition_df["broker_id"] = partition_df["broker_id"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["broker_timestamp"] = partition_df["broker_timestamp"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["broker_country_residency"] = partition_df["broker_country_residency"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["broker_ip_country"] = partition_df["broker_ip_country"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["important_score"] = partition_df["important_score"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["important_score_bucket"] = partition_df["important_score_bucket"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["conversion_seconds"] = partition_df["conversion_seconds"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            partition_df["is_converted"] = partition_df["is_converted"].mask(partition_df["conversion_seconds"] != partition_df["min_conversion_seconds"], np.nan)
            
            partition_df.drop(columns=['min_conversion_seconds'])

            table = pa.Table.from_pandas(partition_df)

            analytics_path = self.BASE_DIR/'python_db'/'analytics_cleaned'/'analytics_brokerchooser_cleaned'
            pq.write_to_dataset(table, root_path=analytics_path, filesystem=None)
            logger.info(f"Processed batch and saved to {analytics_path}")

    def create_staging_parquet(self):
        '''
        Method to create all staging parquet files

        Args:
            None

        Returns:
            None
        '''
        self.staging_broker_data()
        self.staging_brokerchooser_conversions()
        self.staging_page_category_mapping()

    def load_staging_parquet(self):
        '''
        Method to load all staging parquet files

        Args:
            None

        Returns:
            None
        '''
        self.load_staging_broker_data()
        self.load_staging_brokerchooser_conversions()
        self.load_staging_page_category_mapping()

    def create_int_parquet(self):
        '''
        Method to create all int parquet files

        Args:
            None

        Returns:
            None
        '''
        self.int_broker_data()
        self.int_brokerchooser_conversions()
        self.int_page_category_mapping()

    def load_int_parquet(self):
        '''
        Method to load all int parquet files

        Args:
            None

        Returns:
            None
        '''
        self.load_int_broker_data()
        self.load_int_brokerchooser_conversions()
        self.load_int_page_category_mapping()

    def create_analytics_parquet(self):
        '''
        Method to create all analytics parquet files

        Args:
            None

        Returns:
            None
        '''
        self.analytics_brokerchooser()

    def load_analytics_parquet(self):
        '''
        Method to load all analytics parquet files

        Args:
            None

        Returns:
            None
        '''
        self.load_analytics_brokerchooser()

    def run_db(self, create_tables=False):
        '''
        Method to run the entire pipeline

        Args:
            create_tables (bool): Whether to create tables

        Returns:
            None
        '''
        if create_tables:
            self.create_staging_parquet()
            self.create_int_parquet()
            self.create_analytics_parquet()

        self.load_staging_parquet()
        self.load_int_parquet()
        self.load_analytics_parquet()

    def run_cleanup(self, create_tables=False):
        '''
        Method to run our cleanup separately

        Created so I can iterate this more quickly
        '''

        if create_tables:
            self.analytics_brokerchooser_cleaned()

        self.analytics_brokerchooser_cleanup()

if __name__ == '__main__':
    handler = PythonDbHandler()
    handler.run_cleanup(create_tables=True)