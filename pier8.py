"""
Obter dados da Pier8 para subir no BigQuery
Criado por: Danilo Steckelberg
Criado para: Evi Brasil
Criado em: 2022-09-08
Referência da API Pier8: http://etracker.pier8.com.br/pier8-v3plus/developer-docs/#api-Introducao
"""

import requests
import pandas as pd
import json
from modulos.utils.projeto import get_config
from datetime import datetime
from modulos.integracoes.storage import google_bigquery 
import xml.etree.ElementTree as ET
import pytz

config = get_config()
datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
class pier8:
    
    BASE_URL = 'https://etracker.pier8.com.br/api/v2/ws/'
    skus = ['001', '002', '003', '1549']
    
    def __init__(self, apikey, token):
        self.apikey = apikey
        self.token = token
        self.GBQ = google_bigquery.GoogleBigQuery()
        self.client = self.GBQ.client

    def obter_movimentacao_estoque_sku(self, sku):
        comp_url = 'consultaEstoque.php?wsdl'
        url = f'{self.BASE_URL}{comp_url}'
        params = f'{{"comando":"2","filtro":[{{"skus":[{{"sku":"{sku}"}}]}}]}}'
        payload = f"""
            <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
                <SOAP-ENV:Body>
                    <apikey>{self.apikey}</apikey>
                    <token>{self.token}</token>
                    <parametros>
                        {params}
                    </parametros>
                </SOAP-ENV:Body>
            </SOAP-ENV:Envelope>
            """
        headers = {'Content-Type': 'text/xml; charset=utf-8'}

        response = requests.request("POST", url, headers=headers, data=payload)

        return(response.text)

    def movimentacao_estoque_sku_to_df(self, sku):
        estoque = self.obter_movimentacao_estoque_sku(sku)
        
        etree = ET.fromstring(estoque)

        df = pd.DataFrame()
        for i in etree.iter(tag='parameters'):

            df = df.append(pd.json_normalize(
                    json.loads(i.text),
                    record_path = ['lote', ['lotes']], 
                    meta = [
                        ['lote','sku'],
                        ['lote','id'],
                        ['lote','descricao'],
                        ['lote','saldodisponivel'],
                        ['lote','saldoempenhado'],
                        ['lote','saldototal'],
                        ['lote','almoxarifado'],
                        ['lote','departamento'],
                        ['lote','atualizacao','date'],
                        ['lote','atualizacao','timezone_type'],
                        ['lote','atualizacao','timezone']
                    ],
                    errors='ignore',sep='_'),
                    ignore_index = True)
        df['sku'] = sku
        df['hash_datetime'] = datetime.strftime(datetime.now(pytz.timezone('America/Sao_Paulo')),'%Y-%m-%d %H:%M:%S')
        return df

    def movimentacao_estoque(self):
        estoque = pd.DataFrame()
        for i in self.skus:
            print(i)
            estoque = estoque.append(
                self.movimentacao_estoque_sku_to_df(i)
            )
        estoque = estoque.drop('validade', axis = 1)
        estoque = estoque.drop(estoque[estoque.numero == 'x'].index)
        return estoque

    def atualizar_estoque_bigquery(self):
        estoque = self.movimentacao_estoque()
        try:
            bq_run = estoque.to_gbq(
                    'estoque.pier',
                    'evi-stitch',
                    chunksize=None,
                    if_exists='append')
            
            msg = f'Adicionar Pier8 ao BQ: OK | msg {bq_run}'
        except:
            msg = f'Adicionar Pier8 ao BQ: Não OK'
        
        return msg


if __name__ == '__main__':
    PIER = pier8(apikei = config['pier8']['apikey'], token = config['pier8']['token'])
    estoque = PIER.atualizar_estoque_bigquery()

