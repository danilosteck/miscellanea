"""
Obter dados do Omie para subir no BigQuery
Criado por: Danilo Steckelberg
Criado para: Evi Brasil
Criado em: 2022-07-21
Referência da API Omie: https://developer.omie.com.br/service-list/
"""

import requests
import pandas as pd
import json
from modulos.utils.projeto import get_config
from datetime import datetime, timedelta
from modulos.integracoes.storage import google_bigquery

config = get_config()


def normalizar_colunas(dataframe, colunas_desejadas):
    """
    Função para eliminar colunas duplicadas e ajudar a garantir sempre 
    o mesmo número de colunas ao carregar os dados pela API.
    Entrada: type(columns) = 'list' : lista com as colunas únicas que serão mantidas.
    Saída: Pandas DataFrame apenas com as colunas selecionadas.
    """
    dataframe = dataframe.loc[:,~dataframe.columns.duplicated()].copy()
    colunas_existentes = dataframe.columns
    colunas_adicionadas = [col for col in colunas_desejadas if(col not in colunas_existentes)]
    dataframe = dataframe.reindex(columns = dataframe.columns.tolist() + colunas_adicionadas)
    
    dataframe = dataframe[colunas_desejadas]
    return dataframe

def unique_columns(df, columns):
    """
    Função para eliminar colunas duplicadas e ajudar a garantir sempre 
    o mesmo número de colunas ao carregar os dados pela API.
    Entrada: type(columns) = 'list' : lista com as colunas únicas que serão mantidas.
    Saída: Pandas DataFrame apenas com as colunas selecionadas.
    """
    
    df_unique = df.loc[:,~df.columns.duplicated()].copy()
    df_unique_sel = df_unique[columns]
    return(df_unique_sel)

class omie:
    
    headers = {'Content-type': 'application/json'}

    BASE_URL = 'https://app.omie.com.br/api/v1/'
    NF_URL = 'produtos/nfconsultar/'
    PEDIDO_URL = 'produtos/pedido/'
    PRODUTOS_URL = 'geral/produtos/'
    CATEGORIAS_URL = 'geral/categorias/'
    ESTOQUE_URL = 'estoque/consulta/'

    def _config_api(self):

        api_config_data = {
            'estoque_movimentacoes' : {
                'url' : f'{self.BASE_URL}{self.ESTOQUE_URL}', 'chamado' : 'ListarMovimentoEstoque', 'chave' : 'movProdutoListar', 'chave_pagina' : 'nPagina', 'chave_tot_pags' : 'nTotPaginas', 'chave_total_registros' : 'nTotRegistros'
                }
            }
        return api_config_data


    def __init__(self, key, secret, dataset = 'omie'):
        self.app_key = key
        self.app_secret = secret
        self.dataset = dataset
        self.GBQ = google_bigquery.GoogleBigQuery()
        self.client = self.GBQ.client

    def _criar_parametros(self, attributes, chamado):
        """ 
        Função que cria os parâmetros que serão utilizados na requisição da API.
        Entradas: type(attributes)='dict'; type(chamado)='str'
        Saída: parâmetros para serem usados de entrada na função de requisições.
        """
        attributes = attributes

        # if 'pagina' not in attributes.keys():
        #     attributes['pagina'] = 1

        # if 'registros_por_pagina' not in attributes.keys():
        #     attributes['registros_por_pagina'] = 10

        data = {
                "call":f"{chamado}",
                "app_key":f"{self.app_key}",
                "app_secret":f"{self.app_secret}",
                "param":[attributes]
                }

        return data

    def _requisicao_api(self, url, attributes,  chamado):
        """
        Função para simplificar requisição de APIs Omie.
        Entradas:
            type(url) = 'str' : string com a URL a ser chamada
            type(attributes) = 'dict' : dict com os atributos que serão utilizados
            type(chamado) = 'str' : string com o parâmetro "call" da API.
        Saída: resposta da requisição com os parâmetros selecionados.
        """
        data = json.dumps(self._criar_parametros(attributes, chamado))
        resposta = requests.post(url, headers=self.headers, data = data)  
        if not resposta.ok:
            print(f'Erro na requisicao de API.\nURL: {url}\nHeaders: {self.headers}')
        return resposta

    def _requisicao_api_recorrente(self, url, attributes,  chamado, chave, chave_pagina = 'pagina', chave_tot_pags = 'total_de_paginas', chave_total_registros = 'total_de_registros'):
        """
        Função para simplificar requisição de APIs Omie para casos com várias páginas.
        Entradas:
            type(url) = 'str' : string com a URL a ser chamada
            type(attributes) = 'dict' : dict com os atributos que serão utilizados
            type(chamado) = 'str' : string com o parâmetro "call" da API.
        Saída: resposta da requisição com os parâmetros selecionados.
        """
        data = {
                "call":f"{chamado}",
                "app_key":f"{self.app_key}",
                "app_secret":f"{self.app_secret}",
                "param":[attributes]
                }
        
        resposta_api = requests.post(url, headers=self.headers, data = json.dumps(data))
        resposta = resposta_api.json()
        if not resposta_api.ok:
            print(f'Erro na requisicao de API.\nURL: {url}\nHeaders: {self.headers}')

        print(f"Total de Registros:{resposta[chave_total_registros]}")

        # Definição da quantidade de páginas que serão consultadas. 
        # O start em range começa em 2 pois a primeira página de consultas já vem na consulta para obter a quantidade de páginas.
        # O stop precisa do +1 pois a função range não inclui o stop, e precisamos inclusive da n-ésima página.
        pags = range(2,resposta[chave_tot_pags]+1)
        dados_json = resposta[chave]
        print(f"Pág: 1 de um total de {resposta[chave_tot_pags]}")

        # Iteração para obtermos todas as páginas de pedidos do período selecionado
        for p in pags:
            print(f"Pág: {p} de um total de {resposta[chave_tot_pags]}")
            attributes.update({chave_pagina:p})
            resposta = requests.post(url, headers=self.headers, data = json.dumps(data)).json()
            dados_json.extend(resposta[chave])

        # Transformar o output em Pandas DataFrame.
        # notas_fiscais_df = self.notas_fiscais_df(notas_fiscais)
        
        print(f'Execução {chamado} OK!')
        return dados_json

    # def _obter_total_registros(self, url, attributes):
    #     """
    #     Função para obter quantidade de páginas de uma requisição de API.
    #     """
    #     response = self._requisicao_api(url, attributes)
    #     return response


    def obter_notas_fiscais(self, attributes):
        """
        Retorna os dados de notas fiscais em json para uma página específica.
        As páginas estão naturalmente ordenadas por ID (crescente)
        Referência: https://app.omie.com.br/api/v1/produtos/nfconsultar/#ListarNF
        Parâmetros obrigatórios: pagina, registros_por_pagina.
        Recomendado adicionar apenas_importado_api para garantir que serão coletadas todas NFs.
        Entradas: type(attributes) = 'dict' : dict com os atributos.
        Saída: JSON com as notas fiscais.
        """
        call = 'ListarNF'
        # attributes.update({"call":"ListarNF"})
        # print(attributes)

        url = f'{self.BASE_URL}{self.NF_URL}'
        response = self._requisicao_api(url, attributes, call)
        notas_fiscais = response.json()
        return notas_fiscais

    def notas_fiscais_df(self, notas_fiscais):
        """
        Converte as NFs em formato não estruturado para formato de Pandas DataFrame,
        para facilitar a adição ao Google BigQuery.
        Entradas: Dados de notas fiscais em JSON.
        Saída: Dados de notas fiscais em Pandas DataFrame
        """
        
        # Obter o campo 'det', que contém as informações com quebra por produtos. 
        # Os campos em "meta" vêm em formato JSON. Vamos precisar criar funções recursivas para transformar campos em colunas.
        det = pd.json_normalize(
            notas_fiscais, 
            record_path = ['det'], 
            meta = ['compl','info','ide','prod', ['total','ICMSTot'], ['total','ISSQNtot'], ['total','retTrib'],'nfDestInt','nfEmitInt','pedido','titulos'], 
            errors='ignore',sep='_')

        # Criar um 'det' limpo, sem as colunas que serão substituídas pelas colunas obtidas a partir dos campos
        det_limpo = det.drop(['total_ICMSTot', 'ide', 'compl','info','prod','total_ISSQNtot', 'total_retTrib','nfDestInt','nfEmitInt','pedido','titulos'], axis = 1)
        det.rename({'info': 'nf_info', 'prod': 'nf_prod'}, axis=1, inplace=True) # renomeando para evitar conflitos com funções info e prod

        # Transformar campos em colunas recursivamente
        compl = det.compl.apply(lambda x:pd.Series(x))
        info = det.nf_info.apply(lambda x:pd.Series(x))
        ide = det.ide.apply(lambda x:pd.Series(x))
        prod = det.nf_prod.apply(lambda x:pd.Series(x))
        nfDest = det.nfDestInt.apply(lambda x:pd.Series(x))
        nfEmit = det.nfEmitInt.apply(lambda x:pd.Series(x))
        pedido = det.pedido.apply(lambda x:pd.Series(x))
        titulos = det.titulos.apply(lambda x:pd.Series(x))
        total_icms = det.total_ICMSTot.apply(lambda x:pd.Series(x))
        total_iss = det.total_ISSQNtot.apply(lambda x:pd.Series(x))
        total_retTrib = det.total_retTrib.apply(lambda x:pd.Series(x))

        # Concatenar em um único df.
        notas_fiscais_df = pd.concat([det_limpo,compl,info,ide,prod,total_icms,total_iss,total_retTrib, nfDest, nfEmit, pedido, titulos],axis=1)

        # Obter os nomes das colunas para normalizar o DF obtido e garantir que as colunas sejam as mesmas
        keys_info = self.GBQ.executar_query(f'select * from {self.dataset}.notas_fiscais limit 0').keys()
        cols = list(keys_info)

        notas_fiscais_out = normalizar_colunas(notas_fiscais_df, cols)

        return notas_fiscais_out

    def obter_notas_fiscais_por_data(self, data_inicio, data_fim, registros_por_pag = 500,
     apenas_importado_api = "N", filtrar_apenas_inclusao = "N", filtrar_apenas_alteracao = "S"):
        """
        Função para obter todas as notas fiscais dos últimos n dias. O default são 15 dias e 500 registros por página.
        Entradas: 
            "registros_por_pagina": int
            "apenas_importado_api": str ("S"/"N")
            "filtrar_apenas_inclusao":str ("S"/"N")
            "filtrar_apenas_alteracao":str ("S"/"N")
            "filtrar_por_data_de": str DD/MM/YYYY
            "filtrar_por_data_ate": str DD/MM/YYYY
        Retorno: arquivo Pandas DataFrame.
        """

        # Definição da data de início da coleta a partir da quantidade de dias. Formato é DD/MM/YYYY, conforme documentação API.
        # data_inicio = (datetime.today() - timedelta(days = n)).strftime('%d/%m/%Y')

        # Criação de um dict de atributos padrão, com objetivo de facilitar a chamada da função, que não necessita necessariamente de inputs.
        
        attributes = {
            "pagina":1,
            "registros_por_pagina":f"{registros_por_pag}",
            "ordenar_por":"CODIGO",
            "apenas_importado_api":f"{apenas_importado_api}",
            "filtrar_apenas_inclusao":f"{filtrar_apenas_inclusao}",
            "filtrar_apenas_alteracao":f"{filtrar_apenas_alteracao}",
            "filtrar_por_data_de": f"{data_inicio}", 
            "filtrar_por_data_ate":f"{data_fim}"
            }

        # Retorno da função de Obter Pedidos da classe OMIE.
        retorno_api = self.obter_notas_fiscais(attributes)
        print(f"Total de Registros:{retorno_api['total_de_registros']}")

        # Definição da quantidade de páginas que serão consultadas. 
        # O start em range começa em 2 pois a primeira página de consultas já vem na consulta para obter a quantidade de páginas.
        # O stop precisa do +1 pois a função range não inclui o stop, e precisamos inclusive da n-ésima página.
        pags = range(2,retorno_api['total_de_paginas']+1)
        notas_fiscais = retorno_api['nfCadastro']
        print(f"Pág: 1 de um total de {retorno_api['total_de_paginas']}")

        # Iteração para obtermos todas as páginas de pedidos do período selecionado
        for p in pags:
            print(f"Pág: {p} de um total de {retorno_api['total_de_paginas']}")
            attributes.update({"pagina":p})
            notas_fiscais.extend(self.obter_notas_fiscais(attributes)['nfCadastro'])

        # Transformar o output em Pandas DataFrame.
        # notas_fiscais_df = self.notas_fiscais_df(notas_fiscais)
        
        print('Execução NF OK!')
        return notas_fiscais

    def adicionar_notas_fiscais_por_data_bq(self, data_inicio, data_fim, nome_tabela):
        
        nfs = self.obter_notas_fiscais_por_data(data_inicio = data_inicio,data_fim = data_fim,filtrar_apenas_alteracao="S", apenas_importado_api="N")
        df = self.notas_fiscais_df(nfs)

        # gbq = self.GBQ.client()
        try:
            df.to_gbq(
                f'{self.dataset}.{nome_tabela}',
                'evi-stitch',
                chunksize=None,
                if_exists='replace')

            msg = 'Adicionar NF ao BQ: OK'
        except: 
            msg = 'Adicionar NF ao BQ: NÃO OK'
        return msg

    def atualizacao_diaria_notas_fiscais(self):
        ultima_data = self.GBQ.executar_query(f"select max(cast(concat(split(dInc,'/')[offset(2)],'-',split(dInc,'/')[offset(1)],'-',split(dInc,'/')[offset(0)]) as date)) `data_inclusao` from {self.dataset}.notas_fiscais")['data_inclusao'][0]
        penultima_data = ultima_data + timedelta(days = -1)
        ultima_data_format = ultima_data.strftime('%d/%m/%Y')
        penultima_data_format = penultima_data.strftime('%d/%m/%Y')
        hoje = datetime.today().strftime('%d/%m/%Y')
        print(hoje)
        print(penultima_data_format)
        qr = self.adicionar_notas_fiscais_por_data_bq(penultima_data_format,hoje,'notas_fiscais_temp')
        self.GBQ.executar_query(f"""
            insert into {self.dataset}.notas_fiscais
            (select * from {self.dataset}.notas_fiscais_temp where nfProdInt_nCodItem not in (select nfProdInt_nCodItem from {self.dataset}.notas_fiscais));
            drop table {self.dataset}.notas_fiscais_temp
        """)
        return qr


    def obter_pedidos(self, attributes):
        """
        Retorna os dados de pedidos em json para uma página específica.
        As páginas estão naturalmente ordenadas por ID (crescente)
        Referência: https://app.omie.com.br/api/v1/produtos/pedido/#ListarPedidos
        Parâmetros obrigatórios: pagina, registros_por_pagina.
        Recomendado adicionar apenas_importado_api para garantir que serão coletadas todas NFs.
        Entradas: type(attributes) = 'dict' : dict com os atributos que serão utilizados.
        Saída: JSON com as notas fiscais.
        """
        
        call = 'ListarPedidos'
        url = f'{self.BASE_URL}{self.PEDIDO_URL}'
        response = self._requisicao_api(url, attributes, call)
        pedidos = response.json()
        return pedidos

    def obter_pedidos_por_data(self, data_inicio, data_fim, registros_por_pag = 500,
     apenas_importado_api = "N", filtrar_apenas_inclusao = "N", filtrar_apenas_alteracao = "S"):
        """
        Função para obter todas as notas fiscais dos últimos n dias. O default são 15 dias e 500 registros por página.
        Entradas: 
            "registros_por_pagina": int
            "apenas_importado_api": str ("S"/"N")
            "filtrar_apenas_inclusao":str ("S"/"N")
            "filtrar_apenas_alteracao":str ("S"/"N")
            "filtrar_por_data_de": str DD/MM/YYYY
            "filtrar_por_data_ate": str DD/MM/YYYY
        Retorno: arquivo Pandas DataFrame.
        """

        # Definição da data de início da coleta a partir da quantidade de dias. Formato é DD/MM/YYYY, conforme documentação API.
        # data_inicio = (datetime.today() - timedelta(days = n)).strftime('%d/%m/%Y')

        # Criação de um dict de atributos padrão, com objetivo de facilitar a chamada da função, que não necessita necessariamente de inputs.
        
        attributes = {
            "pagina":1,
            "registros_por_pagina":f"{registros_por_pag}",
            "ordenar_por":"CODIGO",
            "apenas_importado_api":f"{apenas_importado_api}",
            "filtrar_apenas_inclusao":f"{filtrar_apenas_inclusao}",
            "filtrar_apenas_alteracao":f"{filtrar_apenas_alteracao}",
            "filtrar_por_data_de": f"{data_inicio}", 
            "filtrar_por_data_ate":f"{data_fim}"
            }

        # Retorno da função de Obter Pedidos da classe OMIE.
        retorno_api = self.obter_pedidos(attributes)
        print(f"Total de Registros:{retorno_api['total_de_registros']}")

        # Definição da quantidade de páginas que serão consultadas. 
        # O start em range começa em 2 pois a primeira página de consultas já vem na consulta para obter a quantidade de páginas.
        # O stop precisa do +1 pois a função range não inclui o stop, e precisamos inclusive da n-ésima página.
        pags = range(2,retorno_api['total_de_paginas']+1)
        pedidos = retorno_api['pedido_venda_produto']
        print(f"Pág: 1 de um total de {retorno_api['total_de_paginas']}")

        # Iteração para obtermos todas as páginas de pedidos do período selecionado
        for p in pags:
            print(f"Pág: {p} de um total de {retorno_api['total_de_paginas']}")
            attributes.update({"pagina":p})
            pedidos.extend(self.obter_pedidos(attributes)['pedido_venda_produto'])

        # Transformar o output em Pandas DataFrame.
        # notas_fiscais_df = self.notas_fiscais_df(notas_fiscais)
        
        print('Execução Pedidos OK!')
        return pedidos
        
    def pedidos_df(self, pedidos):
        """
        Converte as NFs em formato não estruturado para formato de Pandas DataFrame,
        para facilitar a adição ao Google BigQuery.
        Entradas: Dados de notas fiscais em JSON.
        Saída: Dados de notas fiscais em Pandas DataFrame
        """
        
        # Obter o campo 'det', que contém as informações com quebra por produtos. 
        # Os campos em "meta" vêm em formato JSON. Vamos precisar criar funções recursivas para transformar campos em colunas.
        det = pd.json_normalize(
            pedidos, 
            record_path = ['det'], 
            meta = ['cabecalho','total_pedido','lista_parcelas','frete', 'infoCadastro', 'informacoes_adicionais', 'observacoes'], 
            errors='ignore',sep='_')

        # Criar um 'det' limpo, sem as colunas que serão substituídas pelas colunas obtidas a partir dos campos
        det_limpo = det.drop(['cabecalho','total_pedido','lista_parcelas','frete', 'infoCadastro', 'informacoes_adicionais','observacoes'], axis = 1)

        # # Transformar campos em colunas recursivamente
        cabecalho = det.cabecalho.apply(lambda x:pd.Series(x))
        total_pedido = det.total_pedido.apply(lambda x:pd.Series(x))
        lista_parcelas = det.lista_parcelas.apply(lambda x:pd.Series(x))
        frete = det.frete.apply(lambda x:pd.Series(x))
        infoCadastro = det.infoCadastro.apply(lambda x:pd.Series(x))
        informacoes_adicionais = det.informacoes_adicionais.apply(lambda x:pd.Series(x))
        observacoes = det.observacoes.apply(lambda x:pd.Series(x))

        # # Concatenar em um único df.
        pedidos_df = pd.concat([det_limpo,cabecalho, total_pedido, lista_parcelas, frete, infoCadastro, informacoes_adicionais, observacoes],axis=1)

        # Obter os nomes das colunas para normalizar o DF obtido e garantir que as colunas sejam as mesmas
        keys_info = self.GBQ.executar_query(f'select * from {self.dataset}.pedidos limit 0').keys()
        cols = list(keys_info)
     
        pedidos_out = normalizar_colunas(pedidos_df,cols)
        pedidos_out[['dCan', 'hCan', 'uCan', 'cImpAPI']] = pedidos_out[['dCan', 'hCan', 'uCan', 'cImpAPI']].astype(str)
        return(pedidos_out)

    def adicionar_pedidos_por_data_bq(self, data_inicio, data_fim, nome_tabela):
        
        pedidos = self.obter_pedidos_por_data(data_inicio = data_inicio,data_fim = data_fim,filtrar_apenas_alteracao="S", apenas_importado_api="N")
        df = self.pedidos_df(pedidos)

        # gbq = self.GBQ.client()
        try:
            df.to_gbq(
                f'{self.dataset}.{nome_tabela}',
                'evi-stitch',
                chunksize=None,
                if_exists='replace',
                location = 'southamerica-east1')

            msg = 'Adicionar Pedidos ao BQ: OK'
        except: 
            msg = 'Adicionar Pedidos ao BQ: NÃO OK'
        return msg

    def atualizacao_diaria_pedidos(self):
        ultima_data = self.GBQ.executar_query(f"select max(cast(concat(split(dInc,'/')[offset(2)],'-',split(dInc,'/')[offset(1)],'-',split(dInc,'/')[offset(0)]) as date)) `data_inclusao` from {self.dataset}.pedidos")['data_inclusao'][0]
        penultima_data = ultima_data + timedelta(days = -1)
        penultima_data_format = penultima_data.strftime('%d/%m/%Y')
        hoje = datetime.today().strftime('%d/%m/%Y')
        qr = self.adicionar_pedidos_por_data_bq(penultima_data_format,hoje,'pedidos_temp')
        self.GBQ.executar_query(f"""
            insert into {self.dataset}.pedidos
            (select * from {self.dataset}.pedidos_temp where ide_codigo_item not in (select ide_codigo_item from {self.dataset}.pedidos));
            drop table {self.dataset}.pedidos_temp
        """)
        return qr  
    
    
    def obter_produtos(self, attributes):
        """
        Retorna os dados de notas fiscais em json para uma página específica.
        As páginas estão naturalmente ordenadas por ID (crescente)
        Referência: https://app.omie.com.br/api/v1/produtos/nfconsultar/#ListarNF
        Parâmetros obrigatórios: pagina, registros_por_pagina.
        Recomendado adicionar apenas_importado_api para garantir que serão coletadas todas NFs.
        Entradas: type(attributes) = 'dict' : dict com os atributos.
        Saída: JSON com as notas fiscais.
        """
        call = 'ListarProdutos'
        # attributes.update({"call":"ListarNF"})
        # print(attributes)

        url = f'{self.BASE_URL}{self.PRODUTOS_URL}'
        response = self._requisicao_api(url, attributes, call)
        produtos = response.json()
        return produtos

    def produtos_df(self, produtos):
        """
        Converte as NFs em formato não estruturado para formato de Pandas DataFrame,
        para facilitar a adição ao Google BigQuery.
        Entradas: Dados de notas fiscais em JSON.
        Saída: Dados de notas fiscais em Pandas DataFrame
        """
        
        # Obter o campo 'det', que contém as informações com quebra por produtos. 
        # Os campos em "meta" vêm em formato JSON. Vamos precisar criar funções recursivas para transformar campos em colunas.
        prod_df = pd.json_normalize(
            produtos, 
            errors='ignore',sep='_')

        # Obter os nomes das colunas para normalizar o DF obtido e garantir que as colunas sejam as mesmas
        keys_info = self.GBQ.executar_query(f'select * from {self.dataset}.produtos limit 0').keys()
        cols = list(keys_info)

        produtos = normalizar_colunas(prod_df, cols)

        return produtos

    def obter_produtos_por_data(self, data_inicio = '01/12/2020', data_fim = '01/12/2030', registros_por_pag = 500,
        apenas_importado_api = "N", apenas_omiepdv = "N"):
        """
        Função para obter todas as notas fiscais dos últimos n dias. O default são 15 dias e 500 registros por página.
        Entradas: 
            "registros_por_pagina": int
            "apenas_importado_api": str ("S"/"N")
            "filtrar_apenas_inclusao":str ("S"/"N")
            "filtrar_apenas_alteracao":str ("S"/"N")
            "filtrar_por_data_de": str DD/MM/YYYY
            "filtrar_por_data_ate": str DD/MM/YYYY
        Retorno: arquivo Pandas DataFrame.
        """

        # Definição da data de início da coleta a partir da quantidade de dias. Formato é DD/MM/YYYY, conforme documentação API.
        # data_inicio = (datetime.today() - timedelta(days = n)).strftime('%d/%m/%Y')

        # Criação de um dict de atributos padrão, com objetivo de facilitar a chamada da função, que não necessita necessariamente de inputs.
        
        attributes = {"pagina": 1, "registros_por_pagina": 100, "apenas_importado_api": f"{apenas_importado_api}", "registros_por_pagina":f"{registros_por_pag}", 
        "filtrar_apenas_omiepdv": f"{apenas_omiepdv}", "filtrar_por_data_de": f"{data_inicio}", "filtrar_por_data_ate":f"{data_fim}"}

        # Retorno da função de Obter Pedidos da classe OMIE.
        retorno_api = self.obter_produtos(attributes)
        print(f"Total de Registros:{retorno_api['total_de_registros']}")

        # Definição da quantidade de páginas que serão consultadas. 
        # O start em range começa em 2 pois a primeira página de consultas já vem na consulta para obter a quantidade de páginas.
        # O stop precisa do +1 pois a função range não inclui o stop, e precisamos inclusive da n-ésima página.
        pags = range(2,retorno_api['total_de_paginas']+1)
        produtos = retorno_api['produto_servico_cadastro']
        print(f"Pág: 1 de um total de {retorno_api['total_de_paginas']}")

        # Iteração para obtermos todas as páginas de pedidos do período selecionado
        for p in pags:
            print(f"Pág: {p} de um total de {retorno_api['total_de_paginas']}")
            attributes.update({"pagina":p})
            produtos.extend(self.obter_produtos(attributes)['produto_servico_cadastro'])

        # Transformar o output em Pandas DataFrame.
        # produtos_df = self.produtos_df(produtos)
        
        print('Execução Produtos OK!')
        return produtos

    def adicionar_produtos_por_data_bq(self, data_inicio = "01/12/2020", data_fim = "01/12/2030", nome_tabela = "produtos_temp"):
        """
        Função para adicionar produtos que foram incluídos ou alterados entre duas datas.
        """
        nfs = self.obter_produtos_por_data(data_inicio = data_inicio,data_fim = data_fim)
        df = self.produtos_df(nfs)

        try:
            df.to_gbq(
                f'{self.dataset}.{nome_tabela}',
                'evi-stitch',
                chunksize=None,
                if_exists='replace')

            msg = 'Adicionar Produtos ao BQ: OK'
        except: 
            msg = 'Adicionar Produtos ao BQ: NÃO OK'
        return msg

    def atualizacao_diaria_produtos(self):
        """
        Função para atualizar produtos diariamente.
        """
        
        # Obter a última data completa que consta no registro do BigQuery e converter em string DD/MM/YYYY
        ultima_data = self.GBQ.executar_query(f"select max(cast(concat(split(info_dInc,'/')[offset(2)],'-',split(info_dInc,'/')[offset(1)],'-',split(info_dInc,'/')[offset(0)]) as date)) `data_inclusao` from {self.dataset}.produtos")['data_inclusao'][0]
        penultima_data = ultima_data + timedelta(days = -1)
        penultima_data_format = penultima_data.strftime('%d/%m/%Y')
        hoje = datetime.today().strftime('%d/%m/%Y')

        # Obter os dados entre a última data completa e a data atual e criar uma tabela temporária no BigQuery
        qr = self.adicionar_produtos_por_data_bq(penultima_data_format,hoje,'produtos_temp')

        # Inserir os dados desta tabela temporária na tabela completa e excluir a tabela temporária.
        self.GBQ.executar_query(f"""
            insert into {self.dataset}.produtos
            (select * from {self.dataset}.produtos_temp where codigo_produto not in (select codigo_produto from {self.dataset}.produtos));
            drop table {self.dataset}.produtos_temp
        """)
        return qr

    def obter_dados_gerais(self, url_comp, chamado, atributos, chave, total_pags_key = 'total_de_paginas', pag_key = 'pagina'):
        """
        Função para obter dados adicionais fornecidos pelas APIs do OMIE, que não requerem atualização diária.
        Inputs: 
            url_comp (string): URL complementar do endpoint, após a URL base (https://app.omie.com.br/api/v1/)
            chamado (string): chamado (OMIE_CALL), que depende da API a ser consultada.
            atributos (dict/json): atributos da consulta da API.
            chave (string): chave do dict/json que corresponde aos dados da coleta.
            total_pags_key (string): chave que aponta para a quantidade de páginas no retorno. Em geral, é "total_de_paginas", mas nem sempre.
            pag_key (string): chave que aponta para a página de referência no retorno. Em geral, é "pagina", mas nem sempre.
        Output:
            dataframe com os dados requisitados.
        """
        url = f'{self.BASE_URL}{url_comp}'
        response = self._requisicao_api(url, atributos, chamado).json()
        pags = range(2,response[total_pags_key]+1)
        dados = response[chave]
        for p in pags:
            print(f"Pág: {p} de um total de {response[total_pags_key]}")
            atributos.update({pag_key:p})
            dados.extend(self._requisicao_api(url, atributos, chamado).json()[chave])
        return dados

    def obter_recebimentos_por_data(self, data_inicio, data_fim):
        """
        Função para atualizar tabela de recebimentos por datas específicas.
        Entradas: 
            data_inicio (string): data a partir do qual serão obtidos os registros (DD/MM/YYYY)
            data_fim (string): data até a qual serão obtidos os registros (DD/MM/YYYY)
        Ref: https://app.omie.com.br/api/v1/produtos/recebimentonfe/#ListarRecebimentos
        """         
        
        # Obter os dados de recebimentos de forma recorrente, para todas as páginas existentes.
        # A API obtém resultados apenas a partir da data considerada de início.

        recebimentos = OMIE._requisicao_api_recorrente(
            url = f'{OMIE.BASE_URL}produtos/recebimentonfe/', 
            attributes = {"nPagina": 1, "nRegistrosPorPagina": 500, "dtEmissaoDe":data_inicio, "dtEmissaoAte":data_fim},  
            chamado = "ListarRecebimentos", 
            chave = 'recebimentos', 
            chave_pagina = 'nPagina', 
            chave_tot_pags = 'nTotalPaginas', 
            chave_total_registros = 'nTotalRegistros')
    
        rec_df = pd.json_normalize(recebimentos, sep = '_')

        # Obter os nomes das colunas para normalizar o DF obtido e garantir que as colunas sejam as mesmas
        keys_info = self.GBQ.executar_query(f'select * from {self.dataset}.recebimentos limit 0').keys()
        cols = list(keys_info)
        
        out = normalizar_colunas(rec_df, cols)
        return out

    def atualizacao_diaria_recebimentos(self):
        """
        Função para atualizar tabela de recebimentos diariamente.
        Ref: https://app.omie.com.br/api/v1/produtos/recebimentonfe/#ListarRecebimentos
        """ 
        ultima_data = self.GBQ.executar_query(f"select max(parse_date('%d/%m/%Y', cabec_dEmissaoNFe)) `data_inclusao` from {self.dataset}.recebimentos")['data_inclusao'][0]
        if ultima_data is None:
            penultima_data = datetime.strptime('2020-01-01', '%Y-%m-%d')
        else:
            penultima_data = ultima_data + timedelta(days = -1)
        
        penultima_data_format = penultima_data.strftime('%d/%m/%Y')
        hoje = datetime.today().strftime('%d/%m/%Y')
        recebimentos = self.obter_recebimentos_por_data(penultima_data_format,hoje)
        print(f'Quant. Recebimentos: {len(recebimentos)}')
        try:
            recebimentos.to_gbq(
                f'{self.dataset}.recebimentos_temp',
                'evi-stitch',
                chunksize=None,
                if_exists='replace',
                location = 'southamerica-east1')
            
            msg = 'Adicionar Recebimentos ao BQ: OK'
            print(msg)
            self.GBQ.executar_query(f"""
                delete from {self.dataset}.recebimentos_temp where concat(cabec_cNumeroNFE,cabec_cCNPJ_CPF) in (select concat(nNF, cnpj_cpf) from {self.dataset}.notas_fiscais);
                insert into {self.dataset}.recebimentos
                (select 
                    cast(cabec_cCNPJ_CPF as	STRING) cabec_cCNPJ_CPF,
                    cast(cabec_cChaveNFe as	STRING) cabec_cChaveNFe,
                    cast(cabec_cEtapa as	STRING) cabec_cEtapa,
                    cast(cabec_cModeloNFe as	STRING) cabec_cModeloNFe,
                    cast(cabec_cNome as	STRING) cabec_cNome,
                    cast(cabec_cNumeroNFe as	STRING) cabec_cNumeroNFe,
                    cast(cabec_cRazaoSocial as	STRING) cabec_cRazaoSocial,
                    cast(cabec_cSerieNFe as	STRING) cabec_cSerieNFe,
                    cast(cabec_dEmissaoNFe as	STRING) cabec_dEmissaoNFe,
                    cast(cabec_nIdFornecedor as	INT64) cabec_nIdFornecedor,
                    cast(cabec_nIdReceb as	INT64) cabec_nIdReceb,
                    cast(cabec_nValorNFe as	FLOAT64) cabec_nValorNFe,
                    cast(infoAdicionais_cCategCompra as	STRING) infoAdicionais_cCategCompra,
                    cast(infoAdicionais_dRegistro as	STRING) infoAdicionais_dRegistro,
                    cast(infoAdicionais_nIdConta as	INT64) infoAdicionais_nIdConta,
                    cast(parcelas_cCodParcela as	STRING) parcelas_cCodParcela,
                    cast(parcelas_nQtdParcela as	INT64) parcelas_nQtdParcela,
                    cast(totais_vAproxTributos as	FLOAT64) totais_vAproxTributos,
                    cast(totais_vTotalCOFINS as	FLOAT64) totais_vTotalCOFINS,
                    cast(totais_vTotalNFe as	FLOAT64) totais_vTotalNFe,
                    cast(totais_vTotalPIS as	FLOAT64) totais_vTotalPIS,
                    cast(totais_vTotalProdutos as	FLOAT64) totais_vTotalProdutos,
                    cast(transporte_cTipoFrete as	STRING) transporte_cTipoFrete,
                    cast(transporte_cEspecieVolume as	STRING) transporte_cEspecieVolume,
                    cast(transporte_cMarcaVolume as	STRING) transporte_cMarcaVolume,
                    cast(transporte_cNumeroVolume as	STRING) transporte_cNumeroVolume,
                    cast(transporte_nIdTransportador as	FLOAT64) transporte_nIdTransportador,
                    cast(transporte_nPesoBruto as	FLOAT64) transporte_nPesoBruto,
                    cast(transporte_nPesoLiquido as	FLOAT64) transporte_nPesoLiquido,
                    cast(transporte_nQtdeVolume as	STRING) transporte_nQtdeVolume,
                    cast(totais_bcICMS as	FLOAT64) totais_bcICMS,
                    cast(totais_vICMS as	FLOAT64) totais_vICMS,
                    cast(totais_vTotalIPI as	FLOAT64) totais_vTotalIPI,
                    cast(totais_vFrete as	FLOAT64) totais_vFrete,
                    cast(totais_bcICMSST as	FLOAT64) totais_bcICMSST,
                    cast(totais_vICMSSubstituicao as	FLOAT64) totais_vICMSSubstituicao,
                    cast(transporte_cPlacaVeiculo as	STRING) transporte_cPlacaVeiculo,
                    cast(transporte_cRNTRC as	STRING) transporte_cRNTRC,
                    cast(transporte_cUFVeiculo as	STRING) transporte_cUFVeiculo,
                    cast(totais_vTotalDescontos as	FLOAT64) totais_vTotalDescontos,
                from {self.dataset}.recebimentos_temp
                where cabec_nIdReceb not in (select cabec_nIdReceb from {self.dataset}.recebimentos)
                );
                drop table {self.dataset}.recebimentos_temp;
            """)
            print('Exclusão recebimentos duplicados OK')
        except: 
            msg = 'Adicionar Recebimentos ao BQ: NÃO OK'
        return msg
        
    def obter_cod_etapas_pedidos(self):
        """
        Função para obter código das etapas de pedidos.
        """ 
        etapas = self.obter_dados_gerais('produtos/etapafat/','ListarEtapasFaturamento',{"pagina": 1, "registros_por_pagina": 100},'cadastros')
        etapas_df = pd.json_normalize(list(filter(None, etapas)), record_path = ['etapas'], meta = ['cCodOperacao','cDescOperacao'],sep = '_')
        out = self.GBQ.dataframe_to_bq(etapas_df, self.dataset,'cod_etapas_pedidos')
        return out

    def obter_situcao_trib_icms(self):
        """
        Função para obter dados da situação tributária do ICMS.
        Ref: https://app.omie.com.br/api/v1/produtos/icmscst/#ListarCST
        """ 
        sit_trib_icms = self.obter_dados_gerais('produtos/icmscst/','ListarCST',{"pagina": 1, "registros_por_pagina": 100},'cadastros')
        sit_trib_icms_df = pd.json_normalize(sit_trib_icms,sep = '_')
        out = self.GBQ.dataframe_to_bq(sit_trib_icms_df, self.dataset,'sit_trib_icms')
        return out

    def obter_categorias_nf(self):
        """
        Função para obter categorias das notas fiscais.
        Ref: https://app.omie.com.br/api/v1/geral/categorias/#ListarCategorias
        """ 
        categorias = self.obter_dados_gerais('geral/categorias/','ListarCategorias',{"pagina": 1, "registros_por_pagina": 500},'categoria_cadastro')
        categorias_df = pd.json_normalize(categorias,sep = '_')
        out = self.GBQ.dataframe_to_bq(categorias_df, self.dataset,'categorias_nf')
        return out

    def obter_cfop(self):
        """
        Função para obter dados do CFOP dos produtos.
        Ref: https://app.omie.com.br/api/v1/produtos/cfop/#ListarCFOP
        """
        cfop = self._requisicao_api_recorrente(
            url = f'{self.BASE_URL}produtos/cfop/', 
            chamado = 'ListarCFOP',
            chave = 'cadastros',
            attributes = {"pagina": 1, "registros_por_pagina": 500})
    
        cfop_df = pd.json_normalize(cfop, sep = '_')
        cfop_keys = self.GBQ.executar_query(f'select * from {self.dataset}.cfop limit 0').keys()
        cols = list(cfop_keys)
        cfop_norm = normalizar_colunas(cfop_df, cols)
        try:
            cfop_norm.to_gbq(
                f'{self.dataset}.cfop',
                'evi-stitch',
                chunksize=None,
                if_exists='replace',
                location = 'southamerica-east1')

            msg = 'Adicionar CFOP ao BQ: OK'
        except: 
            msg = 'Adicionar CFOP ao BQ: NÃO OK'
        return msg

    def obter_clientes_por_data(self,data_inicio, data_fim):
        """
        Função para atualizar tabela de clientes por datas específicas.
        Entradas: 
            data_inicio (string): data a partir do qual serão obtidos os registros (DD/MM/YYYY)
            data_fim (string): data até a qual serão obtidos os registros (DD/MM/YYYY)
        Ref: https://app.omie.com.br/api/v1/geral/clientes/#clientes_list_request
        """         
        
        # Obter os dados de recebimentos de forma recorrente, para todas as páginas existentes.
        # A API obtém resultados apenas a partir da data considerada de início.

        clientes = self._requisicao_api_recorrente(
            url = f'{self.BASE_URL}geral/clientes/', 
            attributes = {"pagina": 1, "registros_por_pagina": 500, "filtrar_por_data_de":data_inicio, "filtrar_por_data_ate":data_fim, "filtrar_apenas_inclusao":"N", "filtrar_apenas_alteracao":"N"},  
            chamado = "ListarClientes", 
            chave = 'clientes_cadastro', 
            chave_pagina = 'pagina', 
            chave_tot_pags = 'total_de_paginas', 
            chave_total_registros = 'total_de_registros')

        df = pd.json_normalize(clientes, sep = '_')
        df['info_dAlt'] = pd.to_datetime(df['info_dAlt'], format='%d/%m/%Y')
        df['info_dInc'] = pd.to_datetime(df['info_dInc'], format='%d/%m/%Y')
        keys_info = self.GBQ.executar_query(f'select * from {self.dataset}.clientes limit 0').keys()
        cols = list(keys_info)
        
        out = normalizar_colunas(df, cols)
        print(out.keys())

        out = df
        return out

    def atualizacao_diaria_clientes(self):
        """
        Função para atualizar tabela de clientes diariamente.
        Ref: https://app.omie.com.br/api/v1/geral/clientes/#clientes_list_request
        """ 
        ultima_data = self.GBQ.executar_query(f"select max(info_dInc) `data_inclusao` from {self.dataset}.clientes")['data_inclusao'][0]
        if ultima_data is None:
            penultima_data = datetime.strptime('2020-01-01', '%Y-%m-%d')
        else:
            penultima_data = ultima_data + timedelta(days = -1)
        
        penultima_data_format = penultima_data.strftime('%d/%m/%Y')
        # penultima_data_format = '01/01/2021'
        print(penultima_data_format)
        hoje = datetime.today().strftime('%d/%m/%Y')
        # hoje = '30/07/2021'
        clientes = self.obter_clientes_por_data(penultima_data_format,hoje)
        keys_info = self.GBQ.executar_query(f'select * from {self.dataset}.clientes limit 0').keys()
        cols = list(keys_info)
        
        clientes = normalizar_colunas(clientes, cols)
        print(clientes.keys())
        
        print(f'Quant. Clientes: {len(clientes)}')
        try:
            clientes.to_gbq(
                f'{self.dataset}.clientes_temp',
                'evi-stitch',
                chunksize=None,
                if_exists='replace',
                location = 'southamerica-east1')
            
            msg = 'Adicionar clientes ao BQ: OK'
            print(msg)
            self.GBQ.executar_query(f"""
            insert into {self.dataset}.clientes
            (select * except(cnae, fax_ddd, fax_numero, telefone2_ddd, telefone2_numero, optante_simples_nacional),
            cast(cnae as string) cnae,
            cast(fax_ddd as string) fax_ddd,
            cast(fax_numero as string) fax_numero,
            cast(telefone2_ddd as string) telefone2_ddd,
            cast(telefone2_numero as string) telefone2_numero,
            cast(optante_simples_nacional as string) optante_simples_nacional 
            from {self.dataset}.clientes_temp where codigo_cliente_omie not in (select codigo_cliente_omie from {self.dataset}.clientes));
            drop table {self.dataset}.clientes_temp
        """)
            print('Exclusão clientes duplicados OK')
        except: 
            msg = 'Adicionar clientes ao BQ: NÃO OK'
            
        return msg

if __name__ == '__main__':
    import time
    start = time.time()
    OMIE =  omie(key = config['omie_estoca']['key'], secret = config['omie_estoca']['secret'])
    nfs = OMIE.obter_notas_fiscais_por_data('06/05/2022','16/10/2022')
    ped = OMIE.obter_pedidos_por_data('06/05/2020','16/10/2022')
    det = pd.json_normalize(nfs, record_path = ['det'])
    pd.json_normalize(nfs)
    det.keys()
