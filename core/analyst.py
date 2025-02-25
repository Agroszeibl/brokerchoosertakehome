'''
Analyst class to run forecasts calc metrics etc
'''

from datetime import timedelta
from pathlib import Path

import scipy.stats as stats
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

from core.db_handler import DbHandler

class Analyst:

    BASE_DIR = Path(__file__).resolve().parent.parent
    SOURCE_FOLDER = BASE_DIR / "db" / "sources"
    DB_PATH = BASE_DIR / "db" / "brokerchooser_db.duckdb"

    def __init__(self):
        
        self.db_handler = DbHandler(
            self.SOURCE_FOLDER,
            self.DB_PATH
        )

    def conversion_count_by_country(self):
        '''
        Query conversion count by country
        To see which country drives most conversions
        '''

        query = '''
            WITH base AS (
            SELECT 
                conversion_country
                , COUNT(1) AS count
            FROM analytics.brokerchooser
            WHERE broker_id IS NOT NULL
            GROUP BY 1
            )

            SELECT
                *
            FROM base
        '''

        return self.db_handler.fetch_df(query)

    def conversion_rates_by_page_cat(self):
        '''
        Calc daily conversion rate by page category.
        Need this to calc linear regressions
        
        Args:
            None
            
        Returns:
            None
        '''

        query = '''
            WITH base AS (
            SELECT 
                page_category
                , DATE_TRUNC('day', created_at) AS date
                , COUNT_IF(broker_id is NOT NULL) AS conversion_count
                , COUNT(1) AS total_count
                , conversion_count / total_count AS conversion_rate
            FROM analytics.brokerchooser
            WHERE 
                is_last_action_by_session = TRUE
            GROUP BY 1, 2
            ORDER BY 2 ASC
            )

            SELECT
                page_category
                , date
                , conversion_rate
            FROM base
        '''

        return self.db_handler.fetch_df(query)
    
    def conversion_rates_by_page_cat_agg(self):
        '''
        Calc daily conversion rate by page category.
        Need this to calc linear regressions
        
        Args:
            None
            
        Returns:
            None
        '''

        query = '''
            WITH base AS (
            SELECT 
                page_category
                , COUNT_IF(broker_id is NOT NULL) AS conversion_count
                , COUNT(1) AS total_count
                , conversion_count / total_count AS conversion_rate
            FROM analytics.brokerchooser
            WHERE is_last_action_by_session = TRUE
            GROUP BY 1
            ORDER BY 3 DESC
            )

            SELECT
                page_category
                , conversion_rate
            FROM base
            --filtering on significantly deviating page categories
            WHERE
                page_category IN (
                'Best brokers - forex general'
                , 'Broker review - scaled commission'
                , 'Broker review - scaled alternatives'
                , 'Broker reviews - general'
                , 'Best brokers - general'
                , 'Best brokers - citizen'
                , 'Country pages'
                , 'Not categorized'
                )
        '''

        return self.db_handler.fetch_df(query)
    
    def conversion_rates_by_ui(self):
        '''
        Calc daily conversion rate by page category.
        Need this to calc linear regressions
        
        Args:
            None
            
        Returns:
            None
        '''

        query = '''
            WITH base AS (
            SELECT 
                ui_element
                , DATE_TRUNC('day', created_at) AS date
                , COUNT_IF(broker_id is NOT NULL) AS conversion_count
                , COUNT(1) AS total_count
                , conversion_count / total_count AS conversion_rate
            FROM analytics.brokerchooser
            WHERE is_last_action_by_session = TRUE
            GROUP BY 1, 2
            ORDER BY 2 ASC
            )

            SELECT
                ui_element
                , date
                , conversion_rate
            FROM base
        '''

        return self.db_handler.fetch_df(query)
    
    def overall_conversion_rate(self):
        '''
        Calc daily conversion rate
        Need this to calc linear regressions
        
        Args:
            None
            
        Returns:
            db handler df object
        '''

        query = '''
            WITH base AS (
            SELECT 
                DATE_TRUNC('day', created_at) AS date
                , COUNT_IF(broker_id is NOT NULL) AS conversion_count
                , COUNT(1) AS total_count
                , conversion_count / total_count AS conversion_rate
            FROM analytics.brokerchooser
            WHERE is_last_action_by_session = TRUE
            GROUP BY 1
            ORDER BY 1 ASC
            )

            SELECT
                date
                , conversion_rate
            FROM base
        '''

        return self.db_handler.fetch_df(query)
    
    def conversion_rates_by_country(self):
        '''
        Calc daily conversion rate by page category.
        Need this to calc linear regressions
        
        Args:
            None
            
        Returns:
            None
        '''

        query = '''
            WITH base AS (
            SELECT 
                conversion_country
                , COUNT_IF(broker_id is NOT NULL) AS conversion_count
                , COUNT(1) AS total_count
                , conversion_count / total_count AS conversion_rate
            FROM analytics.brokerchooser
            WHERE is_last_action_by_session = TRUE
            GROUP BY 1
            ORDER BY 3 DESC
            )

            SELECT
                *
            FROM base
        '''

        return self.db_handler.fetch_df(query)
    
    def conv_rate_linreg(self, lookahead=60):
        '''
        Calcs linreg for x periods

        Args:
            df: pandas Dataframe
            lookahead: number of periods to calc regression for

        Returns:
            df: pandas Dataframe
        '''

        df = self.overall_conversion_rate()

        X = (df["date"] - df["date"].min()).dt.days.values.reshape(-1, 1)
        y = df["conversion_rate"].values

        model = LinearRegression().fit(X, y)

        last_date = df["date"].max()
        future_dates = [last_date + timedelta(days=i) for i in range(1, lookahead + 1)]
        X_future = np.array([(date - df["date"].min()).days for date in future_dates]).reshape(-1, 1)

        y_future_pred = model.predict(X_future)

        df_future = pd.DataFrame({
            "date": future_dates,
            "conversion_rate": y_future_pred,
            "source": "Regression (Future)"
        })

        df["source"] = "Actual"
        df = pd.concat([df, df_future], ignore_index=True)

        return df

    def linreg_page_cat(self, lookahead=21):
        '''
        Calcs linreg for x periods

        Args:
            df: pandas Dataframe
            lookahead: number of periods to calc regression for

        Returns:
            df: pandas Dataframe
        '''

        df = self.conversion_rates_by_page_cat()

        future_predictions = []

        for category, group in df.groupby("page_category"):
            X = (group["date"] - group["date"].min()).dt.days.values.reshape(-1, 1)
            y = group["conversion_rate"].values

            # Fit linear regression model
            model = LinearRegression().fit(X, y)

            last_date = group["date"].max()
            future_dates = [last_date + timedelta(days=i) for i in range(1, lookahead + 1)]
            X_future = np.array([(date - group["date"].min()).days for date in future_dates]).reshape(-1, 1)

            y_future_pred = model.predict(X_future)

            future_df = pd.DataFrame({
                "page_category": category,
                "date": future_dates,
                "conversion_rate": y_future_pred,
                "source": "Regression (Future)"
            })
            future_predictions.append(future_df)

        df_future = pd.concat(future_predictions, ignore_index=True)
        df = pd.concat([df, df_future], ignore_index=True)

        return df
    
    def linreg_ui_element(self, lookahead=21):
        '''
        Calcs linreg for x periods

        Args:
            df: pandas Dataframe
            lookahead: number of periods to calc regression for

        Returns:
            df: pandas Dataframe
        '''

        df = self.conversion_rates_by_ui()

        future_predictions = []

        for ui_element, group in df.groupby("ui_element"):
            X = (group["date"] - group["date"].min()).dt.days.values.reshape(-1, 1)
            y = group["conversion_rate"].values

            # Fit linear regression model
            model = LinearRegression().fit(X, y)

            last_date = group["date"].max()
            future_dates = [last_date + timedelta(days=i) for i in range(1, lookahead + 1)]
            X_future = np.array([(date - group["date"].min()).days for date in future_dates]).reshape(-1, 1)

            y_future_pred = model.predict(X_future)

            future_df = pd.DataFrame({
                "ui_element": ui_element,
                "date": future_dates,
                "conversion_rate": y_future_pred,
                "source": "Regression (Future)"
            })
            future_predictions.append(future_df)

        df_future = pd.concat(future_predictions, ignore_index=True)
        df = pd.concat([df, df_future], ignore_index=True)

        return df

    def conversion_by_page(self):
        '''
        Using this to calc conversion by page category
        '''

        query = '''
        SELECT 
            page_category
            , COUNT_IF(broker_id is NOT NULL) AS conversion_count
            , COUNT(1) AS total_count
        FROM analytics.brokerchooser
        WHERE is_last_action_by_session = TRUE
        GROUP BY 1
        '''

        return self.db_handler.fetch_df(query)
    
    def conversion_by_ui_element(self):
        '''
        Using this to calc conversion by page category
        '''

        query = '''
        SELECT 
            ui_element
            , COUNT_IF(broker_id is NOT NULL) AS conversion_count
            , COUNT(1) AS total_count
            , conversion_count / total_count AS conversion_rate
        FROM analytics.brokerchooser
        WHERE is_last_action_by_session = TRUE
        GROUP BY 1
        ORDER BY 4 DESC
        '''

        return self.db_handler.fetch_df(query)
    
    def khi_square_row(self, row, total_true, total, true_col, total_col):
        '''
        Calcs khi square for each row in a dataframe
        '''
        contingency_table = [[row[true_col], total_true - row[true_col]], 
                             [row[total_col], total - row[total_col]]]
        
        chi2, p, _, _ = stats.chi2_contingency(contingency_table)

        return chi2, p
        
    def calc_khi_square(self):
        '''
        Calcs khi square test

        Args:
            df: pandas Dataframe
            cols: list of columns to calc khi square for

        Returns:
            df: pandas Dataframe
        '''

        df = self.conversion_by_page()
        df['chi2'] = df.apply(lambda x: self.khi_square_row(x, df['conversion_count'].sum(), df['total_count'].sum(), 'conversion_count', 'total_count')[0], axis=1)
        df['p_value'] = df.apply(lambda x: self.khi_square_row(x, df['conversion_count'].sum(), df['total_count'].sum(), 'conversion_count', 'total_count')[1], axis=1)
        df['is_significant'] = df['p_value'] < 0.05

        return df
