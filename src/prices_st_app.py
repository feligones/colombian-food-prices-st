import os
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import boto3
import pandas as pd
from dotenv import load_dotenv

# Importing custom modules
from conf import utils as uts

sipsa_link = "https://www.dane.gov.co/index.php/estadisticas-por-tema/agropecuario/sistema-de-informacion-de-precios-sipsa/servicio-web-para-consulta-de-la-base-de-datos-de-sipsa"

# Function to load and process data, and then cache it
@st.cache_data
def load_and_process_data():
    """
    Load and process data from S3, and cache the result.

    Returns:
        pd.DataFrame: Processed DataFrame containing the data.
    """
    # Load ENV secrets
    if not os.getenv("AWS_ACCESS_KEY"):
        load_dotenv()

    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    AWS_PROJECT_PATH = os.getenv("AWS_PROJECT_PATH")

    # Create an S3 client
    s3_client = boto3.client(
        's3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    )

    # Load the dataset from S3
    objects = s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix = AWS_PROJECT_PATH)
    pqt_objects = [obj for obj in objects['Contents'] if obj['Key'].endswith('.parquet')]
    last_pqt_object = sorted(pqt_objects, key=lambda x: x['LastModified'], reverse=True)[0]

    print(last_pqt_object)

    s3_client.download_file(Filename = 'data.parquet', Bucket=AWS_BUCKET_NAME, Key=last_pqt_object['Key'])

    df = pd.read_parquet('data.parquet')

    os.remove('data.parquet')

    return df

# Set Streamlit app title and layout
st.title("Food Price Tracker at Colombian Wholesale Markets \U0001F1E8\U0001F1F4")

# Add caption
st.caption("by: Felipe Gonzalez Esquivel - Data Scientist @ Factored.ai | Economist")

# Add App description
st.markdown(
    f"""
    [ENG]
    This is the *Food Price Tracker at Colombian Wholesale Markets* app. 
    The app allows you to visualize and analyze price data of products in different 
    wholesale markets in Colombia. You can select a wholesale market and a product 
    to see its price behavior over time, monthly and yearly changes, 
    and compare multiple wholesale markets.
    The prices are extracted from the Departamento Administrativo Nacional de Estadística (DANE) 
    [SIPSA report]({sipsa_link}).
    
    [ESP]
    Esta es la aplicación *Termómetro de Precios en las Centrales de Abasto de Colombia*. 
    La aplicación te permite visualizar y analizar datos de precios de productos en diferentes 
    centrales de abasto en Colombia. Puedes seleccionar una central de abasto y un producto 
    para ver su comportamiento de precios a lo largo del tiempo, los cambios mensuales y anuales, 
    y comparar múltiples centrales de abasto.
    Los precios se obtienen del [reporte SIPSA]({sipsa_link}) elaborado por el Departamento Administrativo Nacional de Estadística (DANE)
    """
)

# Load Prices Dataframe
prices_dataframe = load_and_process_data()

# Get unique markets and products in the dataset
unique_markets = prices_dataframe['market'].unique()
unique_products = prices_dataframe['product'].unique()

# List of most important markets for comparison
most_important_markets = [
    'medellin central mayorista de antioquia', 
    'bogota dc corabastos',
    'cucuta cenabastos',
    'barranquilla barranquillita',
    'cali cavasa'
]

# Select market using a dropdown
chosen_market = st.selectbox("Wholesale Market / Central de Abasto", [s.upper() for s in sorted(unique_markets)], index=0)

# Filter the dataset by the chosen market
dff = prices_dataframe[prices_dataframe['market'] == chosen_market.lower()]
products_of_market = sorted(dff['product'].unique())
chosen_product = st.selectbox("Product / Producto", [s.upper() for s in products_of_market], index=0)

# Filter the dataset by the selected market and product
filter_df = prices_dataframe.loc[
    (prices_dataframe['product'] == chosen_product.lower()) &
    (prices_dataframe['market'] == chosen_market.lower())
].copy()
filter_df.sort_values('date', inplace=True)
filter_df['mean_price_diff_m'] = 100 * filter_df['mean_price'].diff(1) / filter_df['mean_price'].shift(1)
filter_df['mean_price_diff_y'] = 100 * filter_df['mean_price'].diff(12) / filter_df['mean_price'].shift(12)
filter_df['color_m'] = filter_df['mean_price_diff_m'].apply(lambda x: 'Positive' if x >= 0 else 'Negative')
filter_df['color_y'] = filter_df['mean_price_diff_y'].apply(lambda x: 'Positive' if x >= 0 else 'Negative')

# Define the color mapping
color_map = {'Positive': '#00b140', 'Negative': '#ff5050'}

# Create a line plot using Plotly Express for mean price over time
fig_level = px.line(filter_df, x='date', y='mean_price', markers=True)
# Set the title and axis labels of the plot
fig_level.update_layout(
    title=f'Price of {chosen_product.upper()} in {chosen_market.upper()}',
    xaxis_title='Fecha',
    yaxis_title='COP $ / Kg',
    template='plotly_white'
)

# Create a bar plot for monthly price changes
fig_change_m = px.bar(filter_df, x='date', y='mean_price_diff_m', color='color_m', color_discrete_map=color_map)
fig_change_m.update_layout(
    title=f'MoM % Price Change of {chosen_product.upper()} in {chosen_market.upper()}',
    xaxis_title='Fecha',
    yaxis_title='MoM %',
    showlegend=False,
    template='plotly_white'
)

# Create a bar plot for annual price changes
fig_change_y = px.bar(filter_df, x='date', y='mean_price_diff_y', color='color_y', color_discrete_map=color_map)
fig_change_y.update_layout(
    title=f'YoY % Price Change of {chosen_product.upper()} in {chosen_market.upper()}',
    xaxis_title='Date',
    yaxis_title='YoY %',
    showlegend=False,
    template='plotly_white'
)

# Create a line plot for multi-market comparison
multi_market_df = prices_dataframe.loc[
    (prices_dataframe['product'] == chosen_product.lower()) & 
    (prices_dataframe['market'].isin(most_important_markets + [chosen_market.lower()]))
].copy()
multi_market_df.sort_values(['market', 'date'], inplace=True)
fig_multi_market = px.line(multi_market_df, x='date', y='mean_price', color='market', markers=True)
fig_multi_market.update_layout(
    title=f"Price of {chosen_product.upper()} across the main Wholesale Markets",
    xaxis_title='Date',
    yaxis_title='COP $ / Kg',
    template='plotly_white',
    legend=dict(orientation='h', yanchor='top', y=-0.2)
)

# Display graphs
st.plotly_chart(fig_level)
st.plotly_chart(fig_change_m)
st.plotly_chart(fig_change_y)
st.plotly_chart(fig_multi_market)