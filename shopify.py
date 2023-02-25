
 # -*- coding: utf-8 -*-
"""
Created by: Danilo Steckelberg
Created to: update Shopify data from csv
Created for: Evi Brasil
Created on: 2021-10-01
Version 2.0 (2021-11-12)
"""

from google.cloud import bigquery as bq
import os
import pandas as pd
import pandas_gbq as pb
import glob
from datetime import datetime

class shopify:
    def __init__(self,credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path + '\\gcp_evi-stitch-fb_api_privatekey.json'
        self.client = bq.Client()
        self.datasets = list(self.client.list_datasets())
        self.project = self.client.project
        self.dataset_id = 'events_shopify_historico'
        self.temp_table_id = 'sales_campaign_temp'
        self.update_table_id = 'sales_campaign'

    # Criação de uma tabela temporária com os últimos 30 dias de transação
    def upload_tabela_temp(self, dataframe):
        try:
            dataframe.to_gbq(self.dataset_id+'.'+self.temp_table_id, 
                  'evi-stitch',
                  chunksize=None,
                  if_exists='replace'
                  )
            print('Upload tabela temp OK')
        except:
            print('Upload tabela temp NOT OK. \nNecessário Debug!')

    # Update da tabela histórica com a tabela atual.
    # São deletados os registros mais recentes para serem atualizados com a tabela temporária.
    def update_tabela_historica(self, queries_path):
        delete_query = 'delete from '+ self.dataset_id + '.' + self.update_table_id + 'where date(`day`) > '\
            'date_add(current_date(), interval -18 day)'
        query_job = self.client.query(delete_query)

        ### ----------- Loading SQL file with MERGE and run query job -----------------
        with open(queries_path + '\\merge_shopify_query.txt') as f:
            contents = f.read()
            try:
                query_job = self.client.query(contents)
                print('OK')
            except:
                print('Erro')

if __name__ == '__main__':
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    download_path = 'C:\\Users\\danil\\Downloads'
    credentials_path = 'C:\\Users\\danil\\Documents\\Danilo\\Evi Data Pipeline\\integracoes\\keys'
    queries_path = 'C:\\Users\\danil\\Documents\\Danilo\\Evi Data Pipeline\\integracoes\\queries'
    logs_path = 'C:\\Users\\danil\\Documents\\Danilo\\Evi Data Pipeline\\integracoes\\logs'

    list_of_files = glob.glob(download_path+'/*.csv')
    latest_file = max(list_of_files, key=os.path.getmtime)
    
    try:
        df = pd.read_csv(latest_file)
        df['day'] = pd.to_datetime(df.day)
        updated_info = len(df['day'])
        csv_read_status = 'ok'
    except:
        updated_info = 0
        csv_read_status = 'erro'

    try:
        shp = shopify(credentials_path)
        shp.upload_tabela_temp(df)
        shp.update_tabela_historica(queries_path)
        query_status = 'ok'
    except:
        query_status = 'erro'

    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(logs_path + "\\shopify_sales_log.txt", "a") as text_file:
        print(f"Start_time: {start_time}, End_time: {end_time}, linhas processadas: {updated_info}, " \
            f"leitura arquivo csv: {csv_read_status}, query status: {query_status}, updated file: {latest_file}.",
        file=text_file)
