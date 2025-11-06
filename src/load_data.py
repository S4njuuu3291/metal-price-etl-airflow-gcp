import logging
from google.cloud import bigquery

def load_data(data_fact_metal_price,data_fact_currency_rates):
    client = bigquery.Client()
    table = "metal-price-pipeline.metal_price_dw.fact_metal_price"
    row = [data_fact_metal_price]
    
    logging.info(f"Load data to Data Warehouse, table: {table}")
    error = client.insert_roows_json(table,row)
    if error == []:
        logging.info(f"✅ New rows have been added to {table}")
    
    table = "metal-price-pipeline.metal_price_dw.fact_currency_rates"
    row = data_fact_currency_rates
    logging.info(f"Load data to Data Warehouse, table: {table}")
    error = client.insert_roows_json(table,row)
    if error == []:
        logging.info(f"✅ New rows have been added to {table}\n")

    
    

