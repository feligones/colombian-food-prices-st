import pandas as pd
import numpy as np
import requests
import os
from unidecode import unidecode
import re
from zeep import Client
from zeep.helpers import serialize_object
import boto3
import nltk
nltk.download('punkt')
from nltk.tokenize import word_tokenize
from dotenv import load_dotenv

# Function to load the dataset of the last month from a SOAP service
def load_prices_dataframe():
    """
    This method returns the dataset of the last month from a SOAP service.

    Args:
        cutoff_date (datetime): cutoff date for the data.

    Returns:
        month_price_data (pd.Dataframe): pandas dataframe containing the data.
    """
    client = Client("https://appweb.dane.gov.co/sipsaWS/SrvSipsaUpraBeanService?WSDL")

    week_price_data = pd.DataFrame(serialize_object(client.service.promediosSipsaSemanaMadr()))
    week_price_data['fechaIni'] = pd.to_datetime(week_price_data['fechaIni'].astype(str).str.split(' ', expand=True)[0])

    rename_columns = {
        'fechaIni': 'date',
        'artiNombre': 'product',
        'fuenNombre': 'market',
        'promedioKg': 'mean_price'
    }
    select_columns = list(rename_columns.values())

    week_price_data = week_price_data.rename(columns=rename_columns)[select_columns]

    # Preprocess text in 'product' and 'market' columns by cleaning the text
    week_price_data['product'] = week_price_data['product'].apply(clean_text)
    week_price_data['market'] = week_price_data['market'].apply(clean_text)

    month_price_data = week_price_data.groupby([pd.Grouper(key='date', freq='MS'), 'product', 'market'])['mean_price'].mean().reset_index()

    return month_price_data

# Function to clean text by removing accents, special characters, and lowercasing
def clean_text(text):
    """
    This method returns the input text without accents, 
    special characters, and lowercased.

    Args:
        text (string): input text.

    Returns:
        clean_text (string): processed text.
    """
    clean_text = unidecode(text.lower())
    clean_text = re.sub(r'[^\w\s]', '', clean_text)
    clean_text = ' '.join(word_tokenize(clean_text))
    return clean_text