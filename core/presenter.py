'''
Class that will handle presentations for us

They are taking metrics and data calculated by our analyst

Then passes it into it's dash dashboard to present to our stakeholders
'''

from pathlib import Path

import pandas as pd
import plotly.express as px
from dash import dcc, html
import dash

from core.db_handler import DbHandler
from core.analyst import Analyst

class Presenter:

    BASE_DIR = Path(__file__).resolve().parent.parent

    def __init__(self):
        '''
        init mostly used to call DbHandler
        '''
        self.db_handler = DbHandler(
            self.BASE_DIR/'db'/'sources',
            self.BASE_DIR/'db'/'brokerchooser_db.duckdb'
        )
        self.analyst = Analyst()

    def weekly_conversions(self):
        '''
        Calc weekly conversions

        Args:
            None

        Returns:
            plotly figure
        '''


        query = '''
        SELECT 
            DATE_TRUNC('WEEK', created_at) AS week
            , COUNT(1) AS count
        FROM analytics.brokerchooser
        WHERE broker_id IS NOT NULL
        GROUP BY 1
        ORDER BY 1 ASC
        '''

        df = self.db_handler.fetch_df(query)

        df['week'] = pd.to_datetime(df['week'])

        fig = px.bar(df, x='week', y='count',
                    title='Weekly Conversion Count')
        
        return fig
    
    def weekly_conversions_by_ui_element(self):
        '''
        Calc weekly conversions by country

        Args:
            None

        Returns:
            plotly figure
        '''


        query = '''
        SELECT 
            DATE_TRUNC('WEEK', created_at) AS week
            , ui_element
            , COUNT(1) AS count
        FROM analytics.brokerchooser
        WHERE broker_id IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1 ASC
        '''

        df = self.db_handler.fetch_df(query)

        df['week'] = pd.to_datetime(df['week'])

        fig = px.bar(df, x='week', y='count', color='ui_element',
                    title='Weekly Conversion Count by Ui Element')
        
        return fig
    
    def weekly_conversions_by_measurement_category(self):
        '''
        Calc weekly conversions by country

        Args:
            None

        Returns:
            plotly figure
        '''


        query = '''
        SELECT 
            DATE_TRUNC('WEEK', created_at) AS week
            , measurement_category
            , COUNT(1) AS count
        FROM analytics.brokerchooser
        WHERE broker_id IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1 ASC
        '''

        df = self.db_handler.fetch_df(query)

        df['week'] = pd.to_datetime(df['week'])

        fig = px.bar(df, x='week', y='count', color='measurement_category',
                    title='Weekly Conversion Count by Measurement Category')
        
        return fig
    
    def high_value_cst_s_by_country(self):
        '''
        To investigate high value cst's by country

        Args:
            None

        Returns:
            plotly figure
        '''

        query = '''
        SELECT 
            conversion_country
            , important_score_bucket
            , COUNT(1) AS count
        FROM analytics.brokerchooser
        WHERE 
            broker_id IS NOT NULL
            AND important_score > 50
        GROUP BY 1, 2
        ORDER BY 1 ASC
        '''

        df = self.db_handler.fetch_df(query)

        fig = px.bar(df, x='conversion_country', y='count', color='important_score_bucket',
                    title='Importance Breakdown by Country',)
        
        return fig
    
    def conversions_by_country(self):
        '''
        Look into overall conversion cont by country

        Args:
            None

        Returns:
            plotly figure
        '''

        df = self.analyst.conversion_count_by_country()

        fig = px.bar(df, x='conversion_country', y='count',
                    title='Conversion Count by Country',)
        
        return fig
    
    def conversions_by_page_category(self):
        '''
        Look into overall conversion cont by country

        Args:
            None

        Returns:
            plotly figure
        '''

        df = self.analyst.conversion_rates_by_page_cat_agg()

        fig = px.bar(df, x='page_category', y='conversion_rate',
                    title='Conversion Rate by Page Category',)
        
        return fig

    def conversion_rate_country(self):
        '''
        Look into overall conversion rate by country

        Args:
            None

        Returns:
            plotly figure
        '''

        df = self.analyst.conversion_rates_by_country()

        fig = px.bar(df, x='conversion_country', y='conversion_rate',
                    title='Conversion Rate by Country',)
        
        return fig
    
    def category_regressions(self):
        '''
        Plotting category regressions
        
        calced by our analyst

        Args:
            None

        Returns:
            plotly figure
        '''

        df = self.analyst.linreg_page_cat()

        fig = px.line(df, x='date', y='conversion_rate', color='page_category',
                    title='Page Category Conversion Rate LinReg Forecast',)
        
        return fig
    
    def ui_regressions(self):
        '''
        Plotting category regressions
        
        calced by our analyst

        Args:
            None

        Returns:
            plotly figure
        '''

        df = self.analyst.linreg_ui_element()

        fig = px.line(df, x='date', y='conversion_rate', color='ui_element',
                    title='Ui Element Conversion Rate LinReg Forecast',)
        
        return fig

    def overall_regression(self):
        '''
        Plotting overall regression
        
        calced by our analyst

        Args:
            None

        Returns:
            plotly figure
        '''

        df = self.analyst.conv_rate_linreg()

        fig = px.line(df, x='date', y='conversion_rate',
                    title='Conversion Rate LinReg Forecast',)
        
        return fig
    
    def khi_conversion_by_page_cat(self):
        '''
        Khi square by page category
        
        Returns:
            fig (plotly figure)
        '''
        df = self.analyst.calc_khi_square()

        fig = px.bar(df, x="page_category", y="chi2", color="is_significant",
                    title="Chi-Square Test Results by Page Category for Conversion Rate",
                    labels={"chi2": "Chi-Square Value", "is_significant": "Statistically Significant"},
                    color_discrete_map={True: "red", False: "blue"})

        for i, row in df.iterrows():
            fig.add_annotation(
                x=row["page_category"],
                y=row["chi2"],
                text=f"p={row['p_value']:.1f}",
                showarrow=True,
                arrowhead=2
            )

        return fig
    
    def run_dashboard(self):
        '''
        Method to create dash instance for running our dashboard

        Args:
            None

        Returns:
            None
        '''
        app = dash.Dash(__name__)

        figures = [
            self.weekly_conversions(),
            self.weekly_conversions_by_ui_element(),
            self.ui_regressions(),
            self.weekly_conversions_by_measurement_category(),
            self.high_value_cst_s_by_country(),
            self.conversions_by_country(),
            self.conversion_rate_country(),
            self.khi_conversion_by_page_cat(),
            self.conversions_by_page_category(),
            self.overall_regression(),
            self.category_regressions(),
        ]

        app.layout = html.Div([
            html.H1("BrokerChooser Conversions Dashboard", style={'textAlign': 'center'}),
            *[dcc.Graph(figure=fig) for fig in figures]
        ])

        app.run_server(debug=True, port=8050)

if __name__ == '__main__':
    presenter = Presenter()
    presenter.run_dashboard()
