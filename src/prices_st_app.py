import os
import numpy as np
import pandas as pd
import streamlit as st

# Importing custom modules
from conf import utils as uts
from conf import settings as sts

# Load Prices Dataframe
prices_dataframe = uts.load_data_s3()

print(prices_dataframe.shape)

# # Page configuration
# st.set_page_config(page_title='Termómetro de Precios', layout='wide')
# 
# # Get unique markets and products in the dataset
# unique_markets = prices_dataframe['market'].unique()
# most_important_markets = [
#     'medellin central mayorista de antioquia',
#     'bogota dc corabastos',
#     'cucuta cenabastos',
#     'barranquilla barranquillita'
# ]
# 
# # Sidebar layout
# st.sidebar.title('Filtros')
# chosen_market = st.sidebar.selectbox('Central de Abasto', sorted(unique_markets), index=0)
# chosen_product = st.sidebar.selectbox('Producto', sorted(prices_dataframe[prices_dataframe['market'] == chosen_market]['product'].unique()))
# 
# # Filter the dataset
# filter_prices_dataframe = prices_dataframe.loc[(prices_dataframe['product'] == chosen_product) & (prices_dataframe['market'] == chosen_market)]
# filter_prices_dataframe.sort_values('date', inplace=True)
# filter_prices_dataframe['mean_price_diff_m'] = 100 * filter_prices_dataframe['mean_price'].diff(1) / filter_prices_dataframe['mean_price'].shift(1)
# filter_prices_dataframe['mean_price_diff_y'] = 100 * filter_prices_dataframe['mean_price'].diff(12) / filter_prices_dataframe['mean_price'].shift(12)
# filter_prices_dataframe['color_m'] = filter_prices_dataframe['mean_price_diff_m'].apply(lambda x: 'Positive' if x >= 0 else 'Negative')
# filter_prices_dataframe['color_y'] = filter_prices_dataframe['mean_price_diff_y'].apply(lambda x: 'Positive' if x >= 0 else 'Negative')
# 
# # Define the color mapping
# color_map = {'Positive': '#00b140', 'Negative': '#ff5050'}
# 
# # Main content layout
# st.title('Termómetro de Precios en las Centrales de Abasto de Colombia')
# st.header('Nivel de Precios')
# fig_level = px.line(filter_prices_dataframe, x='date', y='mean_price', markers=True, title=f'Nivel de Precios de {chosen_product.upper()} en {chosen_market.upper()}')
# st.plotly_chart(fig_level)
# 
# st.header('Cambio % Mensual')
# fig_change_m = px.bar(filter_prices_dataframe, x='date', y='mean_price_diff_m', color='color_m', color_discrete_map=color_map, title=f'Cambio % Mensual de {chosen_product.upper()} en {chosen_market.upper()}')
# st.plotly_chart(fig_change_m)
# 
# st.header('Cambio % Anual')
# fig_change_y = px.bar(filter_prices_dataframe, x='date', y='mean_price_diff_y', color='color_y', color_discrete_map=color_map, title=f'Cambio % Anual de {chosen_product.upper()} en {chosen_market.upper()}')
# st.plotly_chart(fig_change_y)
# 
# # Multi-market behavior
# st.header('Comportamiento del Precio en las Centrales de Abasto')
# most_important_markets.append(chosen_market)
# multi_market_prices_dataframe = prices_dataframe.loc[(prices_dataframe['product'] == chosen_product) & (prices_dataframe['market'].isin(most_important_markets))]
# multi_market_prices_dataframe.sort_values(['market', 'date'], inplace=True)
# fig_multi_market = px.line(multi_market_prices_dataframe, x='date', y='mean_price', color='market', markers=True,
#                            title=f'Comportamiento del Precio de {chosen_product.upper()} en las Centrales de Abasto de Colombia')
# st.plotly_chart(fig_multi_market)