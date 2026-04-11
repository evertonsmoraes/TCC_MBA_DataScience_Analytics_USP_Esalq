# -*- coding: utf-8 -*-

"""
Created on 2025-09-19 20:04
Last Updated on 2026-04-11 11:35

@author: Everton S. Moraes
Autor: Everton S. Moraes
Versão: 0.0.1

Projeto: Clusterização de Municípios Brasileiros com Base em Indicadores Socioeconômicos Públicos
GitHub: https://github.com/evertonsmoraes/TCC_MBA_DataScience_Analytics_USP_Esalq

Descrição:
Biblioteca com funções para o TCC de Data Sciense e Analytics para suporte a coleta, tratamento, análise e visualização
de dados socioeconômicos do IBGE, com foco em aplicações de Data Science e
clusterização de municípios brasileiros.

Este módulo foi projetado para ser reutilizado em múltiplos scripts do projeto,
centralizando funções comuns de:
- Integração com API do IBGE
- Manipulação de arquivos (CSV, PKL)
- Logging de execução
- Visualização de dados
- Clusterização e métricas de avaliação

"""


# Detalhes da Biblioteca
__name__= "funcoes_tcc" 
__version__ = '0.0.1' 
__author__ = "Everton S. Moraes"
__email__  = "evertondasilvamoraes@gmail.com.br"
__description__ = ("""Biblioteca Python desenvolvida como parte do TCC de Data Science e Analytics,
 com foco em análise, clusterização e visualização de dados socioeconômicos do IBGE""")
__long_description__ = """
Biblioteca desenvolvida como parte de um Trabalho de Conclusão de Curso (TCC)
em Data Science e Analytics, com o objetivo de apoiar a coleta, tratamento,
análise e clusterização de dados socioeconômicos públicos do Brasil (IBGE).

Funcionalidades principais:
- Integração com API do IBGE
- Coleta e estruturação de indicadores socioeconômicos
- Tratamento e transformação de dados
- Clusterização (K-Means e Gaussian Mixture Models)
- Avaliação de clusters (Silhouette, BIC, Jaccard)
- Geração de visualizações (gráficos e mapas de calor)
- Logging e controle de execução

O projeto foi desenvolvido com foco acadêmico, buscando boas práticas
de engenharia de dados e ciência de dados, permitindo reutilização em outros contextos.
"""
__license__ = "MIT"
__url__ = "https://github.com/evertonsmoraes/TCC_MBA_DataScience_Analytics_USP_Esalq"
__download_url__ = "https://github.com/evertonsmoraes/TCC_MBA_DataScience_Analytics_USP_Esalq/blob/main/funcoes_tcc.py"
__install_requires__ = [
    "requests>=2.28.0",
    "pandas>=1.5.0",
    "numpy>=1.23.0",
    "matplotlib>=3.6.0",
    "seaborn>=0.12.0",
    "scikit-learn>=1.2.0",
    "pillow>=9.0.0",
    "pytz>=2022.1"
]
__keywords__ = [
    "data-science",
    "machine-learning",
    "clustering",
    "kmeans",
    "unsupervised-learning",
    "data-analysis",
    "data-processing",
    "data-visualization",
    "etl",
    "api-integration",
    "ibge",
    "brazil-data",
    "public-data",
    "socioeconomic-data",
    "municipal-data",
    "model-evaluation",
    "silhouette-score",
    "jaccard-index",
    "tcc",
    "academic-project"
]
__classifiers__ = [
    "Development Status :: 4 - Beta",

    # Público
    "Intended Audience :: Education",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Developers",

    # Tema
    "Topic :: Scientific/Engineering :: Information Analysis",
    "Topic :: Scientific/Engineering :: Artificial Intelligence",
    "Topic :: Software Development :: Libraries :: Python Modules",

    # Linguagem
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",

    # OS
    "Operating System :: OS Independent",

    # Licença
    "License :: OSI Approved :: MIT License",
]


# Importações
import requests
import pandas as pd
import json
import time
import datetime
import os
from pytz import timezone
import unicodedata
import re
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import ast
from matplotlib.colors import LinearSegmentedColormap
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from PIL import Image
import math
import pickle
from pathlib import Path


# Parâmetros Globais
diretorio = Path(__file__).resolve().parent # diretorio deste arquivo .py
fuso_horario = timezone('America/Sao_Paulo')
ls_log = []
simbolos_sentidos = {
    "decrescente": "▼"
    ,"crescente": "▲"
}
sentido_metricas = {
    "Razao_Variancia": "decrescente"
    ,"Inercia": "crescente"
    ,"BIC": "crescente"
    ,"Silhouette_Score": "decrescente"
    ,"Indice_Jaccard": "decrescente"
}



# Função para exibir o log
def log (mensagem):
  """
  Função para gerar o log/acompanhamento das execuções

  Args:
      mensagem (str): O caminho da pasta atual.
  """
  agora = datetime.datetime.now().astimezone(fuso_horario).strftime("%d/%m/%Y %H:%M:%S")
  print(f'{agora} - {mensagem}')
  ls_log.append(f'{agora} - {mensagem}')


def tratar_nome_arquivo(texto):
    """
    Trata um texto em minúsculas, sem acentuação e sem espaços para utilizar como nome de arquivo

    Args:
      texto(str): texto a ser tratado
      
    Returns:
        str: texto tratado
    """
    # Normaliza para decompor caracteres acentuados
    texto_tratado = unicodedata.normalize('NFKD', texto)
    
    # Remove os caracteres de acento (diacríticos)
    texto_tratado = ''.join(c for c in texto_tratado if not unicodedata.combining(c))
    
    # Converte para minúsculas
    texto_tratado = texto_tratado.lower()
    
    # Substitui espaços (todos os espaços em branco) por "_"
    texto_tratado = re.sub(r'\s+', '_', texto_tratado)
    
    # Remove aspas simples e duplas
    texto_tratado = texto_tratado.replace("'", "").replace('"', "")
    
    return texto_tratado

def verificar_importar_arquivo_csv (caminho,arquivo,importa_registros = False, exibir_log=True):
    """
    Verificar se o arquivo CSV existe e realiza importação
    
    Args:
        caminho(str): caminho do arquivo CSV
        arquivo (str): nome do arquivo CSV
        importa_registros (bool): indicar se os registros do arquivo seja importado
        exibir_log (boll): indicar se deve ser gerado/exibido log
      
    Returns:
        True / False: Verifica se o arquivo existe
        None / DataFrame: DataFrame com os dados do arquivo caso exista
    
    """
    # Verifica se arquivo existe
    caminho_completo = caminho + "/" + arquivo
    if os.path.exists(caminho_completo):
        
        # Importa os registros
        if importa_registros == True:
            df_retorno = pd.read_csv(caminho_completo, sep=";" , encoding="utf-8")        
            if exibir_log:
                log(f"✅ Arquivo '{caminho_completo}' localizado e importado com sucesso !!")
            return True, df_retorno
        else:
            if exibir_log:
                log(f"✅ Arquivo '{caminho_completo}' localizado com sucesso !!")
            return True, pd.DataFrame()
    else:
        log(f"❌ Erro !! O arquivo {caminho_completo} não foi localizado")
        return False, pd.DataFrame()


def exportar_pickle(dados,caminho,arquivo):
    """
    Exportar os dados para um arquivo PKL
    
    Args:
        dados(): Dados a ser exportado
        caminho(str): caminho do arquivo PKL a ser gerado
        arquivo (str): nome do arquivo PKL a ser gerado
    
    """
    
    caminho_completo = caminho_completo = caminho + "/" + arquivo
    with open(caminho_completo, "wb") as f:
        pickle.dump(dados, f)

def importar_pickle(caminho,arquivo):
    """
    Exportar os dados para um arquivo PKL
    
    Args:
        caminho(str): caminho do arquivo PKL a ser gerado
        arquivo (str): nome do arquivo PKL a ser gerado
     
    Returns:
        Dados do aqruivo
    """
    
    caminho_completo = caminho_completo = caminho + "/" + arquivo
    
    with open(caminho_completo, "rb") as f:
        dados_importados = pickle.load(f)
    
    return dados_importados

def exportar_dataframe_csv (df,caminho,arquivo, incluir_index=False) :
    """
    Exportar o DataFrame para um arquivo CSV
    
    Args:
        df(DataFrame): DataFrame a ser exportado
        caminho(str): caminho do arquivo CSV a ser gerado
        arquivo (str): nome do arquivo CSV a ser gerado
    
    """
    
    caminho_completo = caminho + "/" + arquivo
    
    df.to_csv(caminho_completo, sep=";", encoding="utf-8", index = incluir_index)
    
    check_arquivo,df = verificar_importar_arquivo_csv(caminho,arquivo,False,False)
    
    if check_arquivo:
        
        log(f"✅ DataFrame exportado com sucesso no arquivo '{caminho_completo}'")
    
    else:
        log("❌ Erro !! ao exportar o arquivo")
    
   
def consultar_existencia_objeto(nome_objeto,contexto,tipo_esperado=None):
    """
    Consulta se um objeto existe no contexto informado, com verificação opcional de tipo.

    Args:
        nome_objeto (str): Nome da variável/objeto a ser consultado.
        contexto (dict): Contexto onde o objeto será procurado (ex: globals(), locals(), dict).
        tipo_esperado (type ou tuple, opcional): Tipo esperado do objeto
            (ex: pd.DataFrame, dict, list). Se None, apenas verifica existência.

    Returns:
        bool: True se o objeto existir (e for do tipo esperado, se informado),
              False caso contrário.
    """

    # Verifica existência pelo nome
    if nome_objeto not in contexto:
        return False

    # Se não houver tipo esperado, basta existir
    if tipo_esperado is None:
        return True

    # Verifica o tipo do objeto
    return isinstance(contexto[nome_objeto], tipo_esperado)
    
def consultar_existencia_dataframe(nome_dataframe,contexto):
    """
    Consultar se o Objeto existe
    
    Args:
        nome_objeto(str): nome do DataFrame a ser consultado
        contexto(): contexto a ser utilizado na consluta
    
    Returns:
        True / False: Indicação se o DataFrame existe
    """
    retorno = consultar_existencia_objeto(nome_dataframe,contexto,tipo_esperado='pd.DataFrame')
    
    return retorno
    
def consultar_conjunto_agregado(tipo_consulta,valor_consulta):
    """
    Consultar conjunto de agregados do TIPO_CONSULTA = VALOR CONSULTA
    Documentação de referencia: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-Agregados
    
    Args:
        tipo_consulta(str): tipo de consulta a ser realizado (periodo,assunto,classificacao,periodicidade,nivel)
        valor_consulta(str): valor de filtro do tipo de consulta
      
    Returns:
        None / DataFrame: DataFrame com os dados do conjunto de agregados
    """
    # documentacao da API
    
    # Valida o tipo_consulta e gera url a ser consultada
    match tipo_consulta:
        case "periodo" | "assunto" | "classificacao" | "periodicidade" | "nivel":
            url = f"https://servicodados.ibge.gov.br/api/v3/agregados?{tipo_consulta}={valor_consulta}"
        case _:
            print("❌ Erro !! Valor inválido no tipo_consulta")
            return None
    
    # Acessa a url
    response = obter_com_retry(url, tentativas=5, espera=10)

    # Se retorno de sucesso
    if response.status_code == 200:
        js_retorno = json.loads(response.text)

        # Cria DataFrame
        df_retorno = pd.DataFrame({
                                    tipo_consulta.upper(): pd.Series(dtype="string")
                                    ,"ID_PESQUISA": pd.Series(dtype="string")
                                    ,"PESQUISA_NOME": pd.Series(dtype="string")
                                    ,"ID_AGREGADO": pd.Series(dtype="int64")
                                    ,"AGREGADO_NOME": pd.Series(dtype="string")
                                    })

        # Loop no json  - Nivel 1 - Pesquisa
        for i_pesq in js_retorno:
            
            # Nivel 1 - Pesquisa
            js_agregados = i_pesq["agregados"]
            
            # Nivel 2 - Agregados
            for i_agre in js_agregados:
                
                df_retorno = pd.concat([df_retorno
                                        ,pd.DataFrame([{
                                            tipo_consulta.upper():valor_consulta
                                            ,"ID_PESQUISA": i_pesq["id"]
                                            ,"PESQUISA_NOME": i_pesq["nome"]
                                            ,"ID_AGREGADO": i_agre["id"]
                                            ,"AGREGADO_NOME":i_agre["nome"]
                                            }])
                                        ]
                                        ,ignore_index=True)
        return df_retorno
                
    else:
        print("❌ Erro !! Erro ao realizar a consulta:", response.status_code)
        return None
                                                          
def consultar_metadados_agregado(id_agregado):
    """
    Consultar metadados do agregado
    Documentação de referencia: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-Metadados
    
    Args:
        id_agregado(str): Id do agregado a ser consultado os metadados
      
    Returns:
        DataFrame: DataFrame com os detalhes do agregado
          
    """
    
        
    # Detalhe Agregado
    url_metadados =f"https://servicodados.ibge.gov.br/api/v3/agregados/{id_agregado}/metadados"
        
    # Acessa a url metadados
    response_metadados = obter_com_retry(url_metadados, tentativas=5, espera=10)
        
    # Valida se tem status_code
    if response_metadados:
    
        # Se retorno de sucesso
        if response_metadados.status_code == 200:
            js_metadado = json.loads(response_metadados.text)
            
            # Dados Gerais
            df_geral = pd.DataFrame([{
                                        "ID_AGREGADO": js_metadado["id"]
                                        ,"AGREGADO_NOME": js_metadado["nome"]
                                        ,"AGREGADO_URL": js_metadado["URL"]
                                        ,"AGREGADO_ASSUNTO": js_metadado["assunto"]
                                        }])
            
            # Gera o DataFrame Consolidado que será consolidado todas as caracteristicas
            df_consolidado = df_geral
                                          
            # Trata Periodicidade
            if js_metadado["periodicidade"]:
                
                df_periocidade = pd.DataFrame([js_metadado["periodicidade"]])
                df_periocidade = df_periocidade.add_prefix("periodo_")
                df_periocidade.columns = df_periocidade.columns.str.upper()
                
                # Consolida de modo matricial df_geral + df_periocidade
                df_consolidado = df_consolidado.merge(df_periocidade, how="cross")
                
            # Trata Nivel Territorial
            if js_metadado["nivelTerritorial"]:
                
                trat_nivel_territorial = []
                
                for nivel, detalhes in  js_metadado["nivelTerritorial"].items():
                    if detalhes:
                        for det in detalhes:
                                trat_nivel_territorial.append({
                                    "Territorio_Nivel": nivel,
                                    "Territorio_Nivel_Detalhe": det
                                    })
                    else :
                        trat_nivel_territorial.append({
                            "Territorio_Nivel": nivel,
                            "Territorio_Nivel_Detalhe": None
                            })
                
                df_nivel_territorial = pd.DataFrame(trat_nivel_territorial)
                df_nivel_territorial.columns = df_nivel_territorial.columns.str.upper()
                
                # Consolida de modo matricial df_geral + df_periocidade
                df_consolidado = df_consolidado.merge(df_nivel_territorial, how="cross")        
                
            # Trata Variaveis
            if js_metadado["variaveis"]:
                
                df_variaveis = pd.DataFrame(js_metadado["variaveis"])
                df_variaveis = df_variaveis.add_prefix("variavel_")
                df_variaveis.columns = df_variaveis.columns.str.upper()
                
                # Consolida de modo matricial df_geral + df_periocidade
                df_consolidado = df_consolidado.merge(df_variaveis, how="cross") 
            
            # Trata Classificacoes
            if js_metadado["classificacoes"]: 
                
                
                # Cria as estruturas
                df_classifcacoes = pd.DataFrame({
                                                "CLASSIFICACAO_ID": pd.Series(dtype="int64")
                                                 ,"CLASSIFICACAO_NOME": pd.Series(dtype="string")
                                                 ,"CLASSIFICACAO_SUMARIZACAO_STATUS": pd.Series(dtype="string")
                                                 ,"CLASSIFICACAO_SUMARIZACAO_EXCECAO": pd.Series(dtype="string")
                                                 ,"CATEGORIA_ID": pd.Series(dtype="int64")
                                                 ,"CATEGORIA_NOME": pd.Series(dtype="string")
                                                 ,"CATEGORIA_UNIDADE": pd.Series(dtype="string")
                                                 ,"CATEGORIA_NIVEL": pd.Series(dtype="string")
                                                 })
                
                df_estr_class = pd.DataFrame({
                                            "CLASSIFICACAO_SUMARIZACAO_STATUS": pd.Series(dtype="string")
                                            ,"CLASSIFICACAO_SUMARIZACAO_EXCECAO": pd.Series(dtype="string")
                                            })
                
                df_estr_class_categ = pd.DataFrame({
                                                    "CATEGORIA_ID": pd.Series(dtype="int64")
                                                    ,"CATEGORIA_NOME": pd.Series(dtype="string")
                                                    ,"CATEGORIA_UNIDADE": pd.Series(dtype="string")
                                                    ,"CATEGORIA_NIVEL": pd.Series(dtype="string")
                                                   })
                
                
                df_class = pd.DataFrame(js_metadado["classificacoes"])
                
                for row in df_class.itertuples(index=False):
                    
                    class_id = row.id
                    class_nome = row.nome
                    class_sumarizacao = row.sumarizacao
                    
                    df_class_macro =  pd.DataFrame([{
                                                "id": class_id
                                                ,"nome": class_nome
                                                ,"sumarizacao": class_sumarizacao
                                                }])
                    if class_sumarizacao:
                        df_class_sum = pd.DataFrame([class_sumarizacao])
                        df_class_sum = df_class_sum.add_prefix("classificacao_sumarizacao_")
                        df_class_sum.columns = df_class_sum.columns.str.upper()
                    else:
                        df_class_sum = df_estr_class
                    
                    class_categorias = row.categorias
                    
                    if class_categorias:
                        df_class_categ = pd.DataFrame(class_categorias)
                        df_class_categ = df_class_categ.add_prefix("categoria_")
                        df_class_categ.columns = df_class_categ.columns.str.upper()
                    else:
                        df_class_categ = df_estr_class_categ
                    
                    # Ajusta nome das colunas do Marco
                    df_class_cons = df_class_macro[["id", "nome"]]
                    df_class_cons = df_class_cons.add_prefix("CLASSIFICACAO_")
                    df_class_cons.columns = df_class_cons.columns.str.upper()
                    
                    # Consolida dados Macros + Sumarizacao
                    df_class_cons = df_class_cons.merge(df_class_sum, how="cross") 
                    
                    # Consolida dados Macros + Sumarizacao + Categorias
                    df_class_cons = df_class_cons.merge(df_class_categ, how="cross") 
                    
                    # Empilha os regitros
                    df_classifcacoes = pd.concat([df_classifcacoes, df_class_cons], axis=0, ignore_index=True)
                
                # Consolida de modo matricial df_geral + df_periocidade
                df_consolidado = df_consolidado.merge(df_classifcacoes, how="cross")
                
            log(f'Obtido com sucesso os metadados do Id_Agregado: {id_agregado}')        
            return df_consolidado
        else:
            log(f'❌ Erro: {response_metadados.status_code} - {response_metadados.text}')
            return None
    else:
        log(f'Sem Retorno de Metadados para o Id_Agregado {id_agregado}')
        return None

def obter_indicadores(id_agregador,periodo,id_variavel,localidade,classificacoes=None):
    """
    Obter os indicadores filtrando Agregado, Período, Variável, Localidade e Classificação
    Documentação de referencia: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-Variaveis-agregadosAgregadoPeriodosPeriodosVariaveisVariavelGet e https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
    
    Args:
        id_agregado(str): Id do agregado a ser obtido indicador
        periodo(str): Id da periodo a ser considerada para obter os indicadores
        variavel(str): Id da variavel a se considerada para obter os indicadores
        classificacao(str): classificação(ões) a ser(em) consideradas/filtradas para obter os indicadores id_classificacao[id_categoria]
        
    Returns:
        True / False: Indicação se o indicadores existem
        None / requests.Response: Retorno da requisição
        DataFrame: DataFrame com os indicadores
    """
    
    # Gera a URL
    url_dados = f"https://servicodados.ibge.gov.br/api/v3/agregados/{id_agregador}/periodos/{periodo}/variaveis/{id_variavel}?localidades={localidade}"
    
    
    # Se preenchido classifacoes
    if classificacoes:
        url_dados = url_dados+f"&classificacao={classificacoes}"
    
        
    # Acessa a url dados
    response_dados = obter_com_retry(url_dados, tentativas=5, espera=10)
        
    # Se teve retorno
    if response_dados:
    
        # Se retorno de sucesso
        if response_dados.status_code == 200:
            
            # Cria DataFrame
            df_retorno = pd.DataFrame({
                                        "ID_VARIAVEL": pd.Series(dtype="int64")
                                        ,"VARIAVEL_NOME": pd.Series(dtype="string")
                                        ,"VAR_UNIDADE": pd.Series(dtype="string")
                                        ,"ID_CLASSIFICACAO": pd.Series(dtype="string")
                                        ,"CLASSIFICACAO_NOME": pd.Series(dtype="string")
                                        ,"ID_CATEGORIA": pd.Series(dtype="string")
                                        ,"CATEGORIA_NOME": pd.Series(dtype="string")
                                        ,"ID_LOCALIDADE": pd.Series(dtype="int64")
                                        ,"ID_NIVEL_LOCALIDADE": pd.Series(dtype="string")
                                        ,"NIVEL_LOCALIDADE_NOME": pd.Series(dtype="string")
                                        ,"NOME_LOCALIDADE": pd.Series(dtype="string")
                                        ,"PERIODO": pd.Series(dtype="string")
                                        ,"VALOR": pd.Series(dtype="string")
                                      })
            

            js_dados = json.loads(response_dados.text)
            
            # Obtem as informacoes gerais
            df_geral = pd.DataFrame(js_dados)
           
            # Gera o DataFrame que irá consolidando os resultados
            df_cons = pd.DataFrame([{
                                        "ID_VARIAVEL": int(df_geral["id"].iloc[0])
                                        ,"VARIAVEL_NOME": str(df_geral["variavel"].iloc[0])
                                        ,"VAR_UNIDADE": str(df_geral["unidade"].iloc[0])
                                        }])
            
            df_resultados = pd.DataFrame(df_geral["resultados"].iloc[0])
            
            
            # Trata Classificacoes
            res_classificacoes = ast.literal_eval(str(df_resultados["classificacoes"].iloc[0]))

            ids_class = " | ".join(item['id'] for item in res_classificacoes)
            class_nomes = " | ".join(item['nome'] for item in res_classificacoes)
            ids_cat = " | ".join( list(item['categoria'].keys())[0] for item in res_classificacoes)
            cat_nome = " | ".join( list(item['categoria'].values())[0] for item in res_classificacoes)
            
            df_class = pd.DataFrame([{
                                        "ID_CLASSIFICACAO": ids_class
                                        ,"CLASSIFICACAO_NOME": class_nomes
                                        ,"ID_CATEGORIA": ids_cat
                                        ,"CATEGORIA_NOME": cat_nome
                                        }])
            
            # Consolida dados Gerais + Classificacao
            df_cons = df_cons.merge(df_class, how="cross")
            
            # Trata as Series
            df_res_series = pd.DataFrame(df_resultados["series"].iloc[0])
            
            # Cria as estruturas para localidade e periodo
            df_local_periodo = pd.DataFrame(columns=["ID_LOCALIDADE"
                                               ,"ID_NIVEL_LOCALIDADE"
                                               ,"NIVEL_LOCALIDADE_NOME"
                                               ,"NOME_LOCALIDADE"
                                               ,"PERIODO"
                                               ,"VALOR"
                                               ])
            
            for linha in df_res_series.itertuples(index=False):
                
                res_localidade = ast.literal_eval(str(linha.localidade))
                
                df_local_atual = pd.DataFrame([{
                            "ID_LOCALIDADE": int(res_localidade["id"]),
                            "ID_NIVEL_LOCALIDADE": str(res_localidade["nivel"]["id"]),
                            "NIVEL_LOCALIDADE_NOME": str(res_localidade["nivel"]["nome"]),
                            "NOME_LOCALIDADE": str(res_localidade["nome"])
                        }])
                
                res_serie = ast.literal_eval(str(linha.serie))
            
                df_periodo_atual = pd.DataFrame([
                                                {
                                                "PERIODO": str(periodo),
                                                "VALOR": str(valor)
                                                 }
                                                 for periodo, valor in res_serie.items()
                                                ]
                                                )
                df_local_periodo_atual = df_local_atual.merge(df_periodo_atual, how="cross")
                
                # Empilha os regitros de localidade e periodo
                df_local_periodo = pd.concat([df_local_periodo, df_local_periodo_atual], axis=0, ignore_index=True)
                
                
            # Consolida dados Gerais + Classificacao + Localidade & Periodo 
            df_cons = df_cons.merge(df_local_periodo, how="cross") 
                
            # Empilha no DataFramde de retorno
            df_retorno = pd.concat([df_retorno, df_cons], axis=0, ignore_index=True)
            
            return True, None, df_retorno
                
                
                
        else:
            return False, response_dados.text ,pd.DataFrame()
    
    # Não teve retorno
    else:
        return False, 'Sem Retorno' ,pd.DataFrame()
    

def obter_com_retry(url, tentativas=3, espera=5):
    """
    Faz uma requisição GET com tentativas de repetição em caso de falha.
    
    Args:
        url (str): URL para a requisição
        tentativas (int): número máximo de tentativas
        espera (int): tempo (segundos) para aguardar entre tentativas
    
    Returns:
        requests.Response | None: Resposta da requisição ou None em caso de erro.
    """
    for tentativa in range(1, tentativas + 1):
        try:
            if tentativa > 1:
                print(f"Tentativa {tentativa}/{tentativas} para {url}")
            response = requests.get(url, timeout=30)  # força timeout de 30s
            response.raise_for_status()  # levanta erro se status != 200
            return response
        except (requests.exceptions.ConnectTimeout, 
                requests.exceptions.ReadTimeout, 
                requests.exceptions.ConnectionError) as e:
            print(f"⚠️ Erro de conexão: {e}")
            if tentativa < tentativas:
                print(f"⚠️ Aguardando {espera} segundos antes de tentar novamente...")
                time.sleep(espera)
            else:
                print("❌ Número máximo de tentativas atingido.")
                return None
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")
            return None


def gerar_histograma(series_dados,titulo=None,titulo_x=None,titulo_y=None,rotacao_x=90,arquivo_saida=None,figsize=(12, 6)):
    """
    Gera histograma bas.

    Args:
        series_dados (Series): Dados de entrada
        titulo (str, opcional): Título do gráfico
        titulo_x (str, opcional): Título do eixo X
        titulo_y (str, opcional): Título do eixo Y
        rotacao_x (int): Rotação dos rótulos do eixo X
        arquivo_saida (str, opcional): Caminho para salvar a imagem
        figsize (tuple): Tamanho da figura
    """

    
    plt.figure(figsize=figsize)
    
    plt.bar(
        series_dados.index,
        series_dados.values
    )
    
    plt.ylabel(titulo_y)
    plt.xlabel(titulo_x)
    plt.title(titulo)
    
    plt.xticks(rotation=rotacao_x)
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(arquivo_saida, dpi=300, bbox_inches="tight")
    log(f"✅ Histograma '{titulo}' gerado: '{arquivo_saida}'")

def gerar_boxplot(df,colunas_filtrar=None,titulo=None,rotacao_x=45,arquivo_saida=None,figsize=(12, 6)):
    """
    Gera o gráfico de boxplot contendo todas as variáveis.

    Args:
        df (DataFrame): Dados de entrada
        colunas_filtrar (list, opcional): Colunas numéricas específicas. Se None, usa todas numéricas.
        titulo (str, opcional): Título do gráfico
        rotacao_x (int): Rotação dos rótulos do eixo X
        arquivo_saida (str, opcional): Caminho para salvar a imagem
        figsize (tuple): Tamanho da figura
    """

    #Seleciona colunas numéricas
    if colunas_filtrar is None:
        colunas_filtrar = df.select_dtypes(include=np.number).columns.tolist()

    if not colunas_filtrar:
        raise ValueError("Nenhuma coluna numérica encontrada para gerar boxplot.")

    #Prepara os dados (lista de arrays)
    dados = [df[col].dropna().values for col in colunas_filtrar]

    # Cria o gráfico
    plt.figure(figsize=figsize)
    plt.boxplot(
        dados,
        labels=colunas_filtrar,
        showfliers=False, # remove outliers
        showmeans=True, #exibe a media
        meanline=True,
        whis=1.5,
        patch_artist=True
    )

    plt.xticks(rotation=rotacao_x, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.5)

    if titulo:
        plt.title(titulo)

    plt.ylabel("Valores")

    plt.tight_layout()

    # Salva se necessário
    if arquivo_saida:
        plt.savefig(arquivo_saida, dpi=300, bbox_inches="tight")

    plt.show()
    plt.close()
    log(f"✅ BloxPlot '{titulo}' gerado: '{arquivo_saida}'")
    


def gerar_mapacalor_indicadores (df_indicadores,titulo,caminho_completo_arquivo, k_destaque = None):
    """
    Função para gerar o mapa de calor com o K e seus indicadires

    Args:
        df_indicadores (DataFrame): DataFrame com o K e seus indicadores
        titulo (str): Titulo a ser incluído na imagem gerada
        caminho_completo_arquivo (str): caminho completo do arquivo a ser gerado com o mapa de calor
        k_destaque (str): K que deve ficar em destaque na imagem gerada

    """
        
    # Configura o DataFrame base e garante que o K esta crescente
    df_valores = df_indicadores.copy()
    df_valores.iloc[:, 0] = df_valores.iloc[:, 0].round(0).astype(int)  # garante k inteiro
    df_valores = df_valores.sort_values(by=df_valores.columns[0]).reset_index(drop=True)
    
    # Identifica a linha do k em destaque (se existir)
    if k_destaque is not None:
        linhas_destaque = df_valores.index[df_valores.iloc[:, 0] == k_destaque].tolist()
    else:
        linhas_destaque = []
        
    # Normaliza apenas as métricas (colunas 1 em diante) 
    df_norm = df_valores.copy()
    for col in df_norm.columns[1:]:
        min_val = df_norm[col].min()
        max_val = df_norm[col].max()
        
        norm = (df_norm[col] - min_val) / (max_val - min_val)
        
        # Inverte a escala se for métrica crescente
        if sentido_metricas.get(col) == "crescente":
            norm = 1 - norm
        
        df_norm[col] = norm
    
    # Verifica as colunas que possuem valor constante, um unico valor
    colunas_constantes = {
        col: df_valores[col].nunique() == 1
        for col in df_valores.columns[1:]
    }
    
    # Cria os mapa de cores
 
    # Cor para texto das colunas constantes
    cor_texto_constante = "#000000" 
    
    # Azul = crescente (▲)
    cmap_crescente = LinearSegmentedColormap.from_list(
        "white_to_blue", ["#FFFFFF", "#5284FF"]
    )
    
    # Vermelho = decrescente (▼)
    cmap_decrescente = LinearSegmentedColormap.from_list(
        "white_to_red", ["#FFFFFF", "#FF5757"]
    )
    
    # Colormap neutro para K
    cmap_neutro = LinearSegmentedColormap.from_list(
        "white_to_gray", ["#FFFFFF", "#8C8C8C"]
    )
    
    
    # Colormap neutro para K
    cmap_k = LinearSegmentedColormap.from_list(
        "white_k", ["#FFFFFF", "#FFFFFF"]
    )
    
    

    # Parametriza casas decimais por coluna
    decimais_por_coluna = {
                            "Razao_Variancia": 4
                           #,"Inercia": 4
                           ,"BIC": 4
                           ,"Indice_Jaccard": 4
                           ,"Silhouette_Score": 4
                               }
    
    # Gera DataFrame a arredondar os  valores
    df_redondado = df_valores.iloc[:, 1:].copy()
    
    # Aplica arredondamento nos valores
    for col, dec in decimais_por_coluna.items():
        df_redondado[col] = df_redondado[col].apply(lambda x: f"{x:.{dec}f}")
    
    # Combina K + metricas arredondadas
    df_redondado_full = pd.concat([df_valores.iloc[:, [0]].astype(str), df_redondado], axis=1)
    
    # Combina K + metricas normalizadas a utilizar como referencia para as cores
    df_refcores = pd.concat([df_valores.iloc[:, [0]], df_norm.iloc[:, 1:]], axis=1)
    
    # Gera colunas com os simbolos ▼ ▲
    colunas_com_simbolo = []

    for col in df_refcores.columns:
        if col == df_refcores.columns[0]:  # coluna K
            colunas_com_simbolo.append(col)
        else:
            # Insere o Simbolo do Sentido
            colunas_com_simbolo.append(f"{col} {simbolos_sentidos.get(sentido_metricas.get(col),'')}")
            
    
    # Matriz de cores
    colors = np.empty(df_refcores.shape, dtype=object)
    
    # Obtendo o K min e max
    k_min, k_max = df_valores.iloc[:,0].min(), df_valores.iloc[:,0].max()
    
    # Matriz de cores para a Coluna k variando conforme valores reais
    for i, val in enumerate(df_valores.iloc[:,0]):
        # normalizando o valor atual
        norm = (val - k_min) / (k_max - k_min) if k_max != k_min else 0
        # gerando a cor
        colors[i,0] = cmap_k(norm)
    
    
    # Matriz de cores para as métricas respeitando o sentido
    for j in range(1, df_refcores.shape[1]):
        col_nome = df_refcores.columns[j]
        sentido = sentido_metricas.get(col_nome)
        
        # Define o colormap conforme o sentido
        if sentido == "crescente":
            cmap_atual = cmap_crescente
        elif sentido == "decrescente":
            cmap_atual = cmap_decrescente
        else:
            cmap_atual = cmap_neutro
        
        
        for i in range(df_refcores.shape[0]):
            colors[i, j] = cmap_atual(df_norm.iloc[i, j])
    
    # Função para criar contraste de texto
    def cor_texto(fundo_rgb):
        r, g, b, *_ = fundo_rgb
        luminancia = 0.299*r + 0.587*g + 0.114*b
        return "black" if luminancia > 0.5 else "white"
    
    # Gera figura
    fig, ax = plt.subplots(figsize=(12, 12))
    sns.set(font_scale=0.9)
    
    # Desenha células
    for i in range(df_refcores.shape[0]):
        for j in range(df_refcores.shape[1]):
            
            col_nome = df_refcores.columns[j]
            
            # desenha retangulo com a cor de fundo
            rect = plt.Rectangle((j, i), 1, 1, facecolor=colors[i,j], edgecolor="#D9D9D9", lw=1)
            ax.add_patch(rect)
            
            # define a cor do texto
            if colunas_constantes.get(col_nome, False):
                cor_txt = cor_texto_constante
            else:
                cor_txt = cor_texto(colors[i, j])
                
            # adiciona o texto com a cor contraste
            ax.text(j+0.5
                    ,i+0.5
                    ,df_redondado_full.iloc[i,j]
                    ,ha="center"
                    ,va="center"
                    ,color = cor_txt
                    ,fontweight="bold" if i in linhas_destaque else "normal" # deixa em negrito se for a linha do k_destaque
                    )
            
            # Simula sublinhado desenhando uma linha abaixo do texto
            if i in linhas_destaque:
                ax.plot(
                    [j + 0.25, j + 0.75]   # comprimento do sublinhado
                    ,[i + 0.93, i + 0.93]   # posição vertical (ajustável)
                    ,color="#8C8C8C" # Cor do sublinhado
                    ,linewidth=1.0 # Espessura do sublinhado
                )
                
    # Ajustes visuais
    ax.set_xlim(0, df_refcores.shape[1])
    ax.set_ylim(0, df_refcores.shape[0])
    ax.invert_yaxis()  # Corrige ordem crescente do K
    
    # Títulos das colunas no topo
    ax.set_xticks(np.arange(df_refcores.shape[1]) + 0.5)
    ax.set_xticklabels(colunas_com_simbolo, ha="center")
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position('top')
    
    # Eixo Y oculto
    ax.set_yticks([])
    
    # Título da figura
    ax.set_title(titulo, fontsize=14, pad=20)
    ax.set_xlabel("")
    ax.set_ylabel("")
    
    plt.tight_layout()
    plt.savefig(caminho_completo_arquivo, dpi=300, bbox_inches="tight")
    plt.show()
    plt.close(fig) # Fecha a figura atual para liberar memória
    sns.reset_orig() # Reseta as configurações para o padrão do Matplotlib
    
    log(f"✅ Mapa de Calor '{titulo}' gerado.")
    #log(f"✅ Mapa de Calor '{titulo}' gerado: '{caminho_completo_arquivo}'")

# Índice Jaccard 
def jaccard_index(pi_r, pi_g, sample_size=3000):
    """
    Calcula o Índice de Jaccard entre duas partições de cluster π_r e π_g,
    Jaccard(π_r, π_g) = np_11 / (np_11 + np_01 + np_10)
    
    Args:
      pi_r (array-like): Vetor de rótulos da partição de referência (π_r).
      pi_g (array-like): Vetor de rótulos da partição comparada (π_g).
      sample_size (int): Tamanho máximo da amostra (opcional).

    Returns:
      float: Valor do Índice de Jaccard entre π_r e π_g.
    """

    n0 = len(pi_r)

    # Amostragem, se necessário
    if sample_size and n0 > sample_size:
        idx = np.random.choice(n0, sample_size, replace=False)
        pi_r = pi_r[idx]
        pi_g = pi_g[idx]

    # Matrizes de coassociação:
    # A_r(i,j) = 1 se i e j pertencem ao mesmo cluster em π_r
    # A_g(i,j) = 1 se i e j pertencem ao mesmo cluster em π_g
    A_r = (pi_r[:, None] == pi_r[None, :])
    A_g = (pi_g[:, None] == pi_g[None, :])

    # np_11: pares no mesmo cluster em ambas as partições
    np_11 = np.logical_and(A_r, A_g).sum()

    # np_10: pares no mesmo cluster em π_r e em clusters diferentes em π_g
    np_10 = np.logical_and(A_r, np.logical_not(A_g)).sum()

    # np_01: pares no mesmo cluster em π_g e em clusters diferentes em π_r
    np_01 = np.logical_and(np.logical_not(A_r), A_g).sum()

    # Índice de Jaccard
    return np_11 / (np_11 + np_01 + np_10)


def gerar_clusters_kmeans(df_variaveis,k_values):
    """
    Gerar clusters utilizando o algoritmo K-Means e calcular métricas de avaliação.

    Args:
      df_variaveis(DataFrame): DataFrame contendo as variáveis para clustering.
      k_values(list): Lista de valores de k (número de clusters) a serem testados.

    Returns:
      df_clusters (DataFrame): DataFrame com as marcações dos clusters para cada valor de k.
      df_metricas (DataFrame): DataFrame com as métricas de avaliação para cada valor de k.

    """
    
    # Para guardar os ndicadores das execucoes
    metricas = [] 
    
    # Para guardas os clusters gerados
    dict_clusters = {}
    
    # Para cálculo do Índice Jaccard entre execuções
    clusters_anterior = None  

    
    # Loop nos ks
    for k in k_values:
        
        log(f"Executando K-Means com {k} clusters...")
    
        # K-Means
        kmeans = KMeans(
                    n_clusters=k
                    ,random_state=42
                    ,n_init=10
                    )
        clusters_atual = kmeans.fit_predict(df_variaveis)
        
        # Guarda os clusters gerados
        dict_clusters[f"cluster_k{k}"] = clusters_atual
        
        ## Métricas
        
        # Inércia -variancia_interna
        inercia = kmeans.inertia_ # soma das distâncias quadradas dentro dos clusters
         
        ## Razão das variâncias (VRC)
        
        # Média global dos dados (x̄)
        x_barra = np.mean(df_variaveis, axis=0)
        
        # BGSS(π) = soma das distâncias quadráticas entre clusters / variancia entre clusters
        BGSS_pi = sum([
                        len(df_variaveis[clusters_atual==i]) 
                        * np.sum((kmeans.cluster_centers_[i] - x_barra)**2)
                        for i in range(k)])
        
        # WGSS(π) = soma das distâncias quadráticas intra-clusters / variancia interna
        WGSS_pi = kmeans.inertia_
        
        # Número total de observações (n₀)
        n0 = len(df_variaveis)
        
        # Variance Ratio Criterion (VRC)
        VRC_pi = (BGSS_pi / WGSS_pi)*((n0 - k)/(k-1))
    
    
    
        ## BIC via GaussianMixture
        gmm = GaussianMixture(n_components=k
                             ,covariance_type='full'
                             ,random_state=42)
        gmm.fit(df_variaveis)
        bic = gmm.bic(df_variaveis)
        
        
        
        ## Silhouette Score
        # Utilizando amostras limitados a 5.000 elementos
        """
        sample_idx = np.random.choice(len(df_variaveis)
                                , min(5000, len(df_variaveis))
                                , replace=False)
        sil = silhouette_score(df_variaveis.iloc[sample_idx]
                               , clusters_atual[sample_idx])
        """
        sil = silhouette_score(df_variaveis
                               ,clusters_atual)

    
           
        ## Jaccard (comparando com cluster anterior, se existir)
        jaccard = np.nan
        if clusters_anterior is not None:
            jaccard = jaccard_index(clusters_anterior
                                    ,clusters_atual
                                    ,sample_size=len(clusters_anterior))
        
        clusters_anterior = clusters_atual  # armazena para próxima comparação
    
    
    
        # Tamanho dos clusters
        tamanhos = np.bincount(clusters_atual)
        tamanho_menor_cluster = tamanhos.min()
        tamanho_maior_cluster = tamanhos.max()
    
        metricas.append({
            'k': k
            ,'Razao_Variancia': VRC_pi # Razão da Variancia BW Ratio
            ,'Inercia' : inercia
            ,'BIC': bic
            ,'Silhouette_Score': sil
            ,'Indice_Jaccard': jaccard
            ,'Tamanho_Menor_Cluster': tamanho_menor_cluster
            ,'Tamanho_Maior_Cluster': tamanho_maior_cluster
            
        })
        
    # Cria DataFrame com as marcações dos clusters
    df_clusters = pd.DataFrame(dict_clusters)

    # Cria DataFrame com as metricas consolidadas
    df_metricas = pd.DataFrame(metricas)
        
    return df_clusters, df_metricas


def gerar_grafico_metricas(df_metricas, inicio_titulo = None, prefixo_arquivo=None, pasta_destino=None):
    """
    Gera gráficos para cada métrica presente em df_metricas, considerando a primeira coluna como eixo X (ex: k), e as demais como métricas numéricas. 
    Se prefixo_arquivo for fornecido, salva os gráficos como imagens na pasta_destino.
    
    Args:
        df_metricas (DataFrame): DataFrame com a primeira coluna como eixo X e as demais como métricas.
        prefixo_arquivo (str, opcional): Prefixo para o nome dos arquivos salvos. Default=None (não salva).
        pasta_destino (str, opcional): Pasta onde os arquivos serão salvos.  (pasta atual).
    """
    
    # trata as colunas 
    colunas = df_metricas.columns
    col_k = colunas[0]
    metrics = colunas[1:]
    
    # configurações dos gráficos
    colors = ['blue', 'green', 'orange', 'red', 'purple', 'brown', 'gray']
    markers = ['o', 's', 'D', 'v', '^', 'x', '*']
    linestyles = ['-', '--', '-.', ':']
    
    # De Para das metricas a utilizar no nome do arquivo
    depara_metrics = {
        'Razao_Variancia': 'variancia'
        ,'Inercia' : 'inercia'
        ,'BIC': 'bic'
        ,'Silhouette_Score': 'silhoutte'
        ,'Indice_Jaccard': 'jaccard'
        ,'Tamanho_Menor_Cluster': 'menor_cluster'
        ,'Tamanho_Maior_Cluster': 'maior_cluster'
        }
    
    # Lista para armazenar os caminhos dos arquivos gerados
    arquivos_gerados = []
    
    # Cria pasta destino se não existir
    if prefixo_arquivo is not None and not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    
    for i, metric in enumerate(metrics):
        plt.figure(figsize=(8, 4))
        plt.plot(
            df_metricas[col_k],
            df_metricas[metric],
            marker=markers[i % len(markers)],
            linestyle=linestyles[i % len(linestyles)],
            color=colors[i % len(colors)],
            markersize=6,
            linewidth=2,
            label=metric
        )
        
        plt.xlabel(col_k)
        plt.ylabel(metric)
        if inicio_titulo:
            titulo_final = f'{inicio_titulo} | {metric} vs {col_k}'
        else:
            titulo_final = f'{metric} vs {col_k}'
        plt.title(titulo_final)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        
        # Obtem o depara da metrica para usar no nome do arquivo
        arq_metric = depara_metrics.get(metric,metric)
        
        # Salva se prefixo fornecido
        if prefixo_arquivo is not None:
            nome_arquivo = f"{prefixo_arquivo}_{tratar_nome_arquivo(arq_metric)}.png"
            caminho_completo = pasta_destino + '/ ' + nome_arquivo
            plt.savefig(caminho_completo, dpi=300, bbox_inches="tight")
            arquivos_gerados.append(caminho_completo)
            log(f"✅ Gráfico '{titulo_final}' gerado: '{caminho_completo}'")
        
        plt.show()
        plt.close()
      
    return arquivos_gerados



def gerar_grafico_metricas_unico(df_metricas, inicio_titulo=None, caminho_destino=None, normalizar=True, melhor_k=None):
    """
    Gera um único gráfico contendo todas as métricas de avaliação
    em função do número de clusters (k).

    Args:
        df_metricas (pd.DataFrame): DataFrame onde a primeira coluna é o eixo X (ex: k) e as demais são métricas numéricas.
        inicio_titulo (str, opcional): Texto inicial do título do gráfico.
        caminho_destino (str, opcional): Caminho completo do arquivo para salvar o gráfico.
        normalizar (bool, opcional): Se True, aplica padronização (Z-score) às métricas.
                                     Default = True.
        melhor_k (int, opcional): Valor de k considerado ótimo, destacado no gráfico.
    """

    colunas = df_metricas.columns
    col_k = colunas[0]
    metricas = colunas[1:]

    # Copia para evitar efeitos colaterais
    df_plot = df_metricas.copy()

    # Normalização das métricas (recomendado)
    if normalizar:
        scaler = StandardScaler()
        df_plot[metricas] = scaler.fit_transform(df_plot[metricas])

    # Estilos visuais
    colors = ['blue', 'green', 'orange', 'gray', 'purple', 'brown', 'cyan']
    markers = ['o', 's', 'D', 'v', '^', 'x', '*']
    linestyles = ['-', '--', '-.', ':']

    plt.figure(figsize=(10, 5))

    for i, metric in enumerate(metricas):
        plt.plot(
            df_plot[col_k],
            df_plot[metric],
            label=metric,
            color=colors[i % len(colors)],
            marker=markers[i % len(markers)],
            linestyle=linestyles[i % len(linestyles)],
            linewidth=2,
            markersize=5
        )
        
        
    # Destaque do melhor k
    if melhor_k is not None:
        plt.axvline(
            x=melhor_k,
            color='red',
            linestyle='--',
            linewidth=1,
            label=f"Melhor k = {melhor_k}"
        )

        # Anotação próxima à linha
        y_max = df_plot[metricas].max().max()
        plt.text(
            melhor_k,
            y_max,
            f"{melhor_k}",
            rotation=90,
            verticalalignment='bottom',
            horizontalalignment='right',
            fontsize=9,
            color='red',
            backgroundcolor='white'
        )
        
    plt.xlabel(col_k)
    plt.ylabel("Métricas (normalizadas)" if normalizar else "Valor da métrica")

    if inicio_titulo:
        plt.title(f"{inicio_titulo} | Métricas vs {col_k}")
    else:
        plt.title(f"Métricas vs {col_k}")

    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(caminho_destino, dpi=300, bbox_inches="tight")
    log(f"✅ Gráfico único de métricas gerado: '{caminho_destino}'")

    plt.show()
    plt.close()    
    
    
    
def identificar_melhor_k(df_metricas):
    """
    Identifica o melhor (k) número de clusters atraves da média normalizada de métricas.
    
    Args:
        df_metricas (pd.DataFrame): DataFrame contendo as métricas de avaliação para cada k.

    Returns:
        melhor_k (int): O valor de k considerado como o mais adequado baseado no score combinado.
        df_norm (pd.DataFrame): DataFrame contendo as métricas de avaliação para cada k normalizadas com o score calculado.
    """
    
    # Copia o DataFrame para evitar risco de alterar o original
    df = df_metricas.copy()
    
    # Determina as metricas a utilizar no score
    metricas_score = ['Razao_Variancia', 'BIC', 'Silhouette_Score', 'Indice_Jaccard']
        
    # Colunas a normalizar, exceto a k
    #colunas_para_normalizar = [col for col in df.columns if col != 'k']
    colunas_para_normalizar = [col for col in metricas_score]
    
    # Pipeline: normalização (MinMax | Scalling)
    pipeline = Pipeline([
        ('scaler', MinMaxScaler())      # MinMax normaliza entre 0 e 1
        #('scaler', StandardScaler())    # Z-Score média = 0, desvio = 1
    ])
    
    # Executa pipeline e normaliza os dados
    df[colunas_para_normalizar] = pipeline.fit_transform(df[colunas_para_normalizar])
    df_norm = df
    
    # Inversão explícita do BIC (métrica de minimização)
    df_norm['BIC'] = 1-df_norm['BIC'] # MinMax 
    #df_norm['BIC'] = -df_norm['BIC'] # Z-Score 
        
    # Cálculo do score (média aritmética)
    #df_norm['Score'] = df_norm[metricas_score].mean(axis=1)
    
    # Cálculo do score ponderado
    df_norm['Score'] = (0.45 * df_norm['Silhouette_Score'])  # 0,45 peso / qualidade interna + separação
    + (0.30 * df_norm['Razao_Variancia']) # 0,30 peso / desigualdade intergrupos
    + (0.15 * df_norm['Indice_Jaccard'])  # 0.15 peso / estabilidade (secundária)
    + (0.10 * df_norm['BIC'])  # 0,10 peso /desempate / simplicidade

   
    
    # Melhor k
    melhor_k = df_norm.loc[df_norm['Score'].idxmax(), 'k']
    log(f"✅ Identificado o melhor k: {melhor_k}'")
    
    return melhor_k, df_norm



def filtrar_e_ordenar_graficos(graficos_gerados):
    """
    Filtra e ordena caminhos de arquivos de gráficos conforme a ordem definida das métricas
    
    Returns:
        graficos_gerados (list): Lista com os caminhos completos dos graficos a serem filtrados e ordenados
    """
    
    
    # Ordem lógica das métricas
    ordem_metricas = [
        #"inercia"
        "variancia"
        ,"bic"
        ,"jaccard"
        ,"silhoutte"
        #,"tamanho_maior_cluster"
    ]
    
    # Dicionário métrica -> lista de arquivos encontrados
    graficos_por_metrica = {metrica: [] for metrica in ordem_metricas}

    for caminho in graficos_gerados:
        for metrica in ordem_metricas:
            if metrica in caminho:
                graficos_por_metrica[metrica].append(caminho)
                break  # evita classificar o mesmo arquivo em duas métricas

    # Consolida na ordem correta
    graficos_ordenados = []
    for metrica in ordem_metricas:
        graficos_ordenados.extend(sorted(graficos_por_metrica[metrica]))

    return graficos_ordenados


def gerar_mosaico(lista_caminhos, qtd_colunas, arquivo_gerar):
    """   
    Cria um mosaico com no máximo 2 colunas e número dinâmico de linhas.
    A ordem da lista define a posição:
        - esquerda → direita
        - de cima para baixo

     Args:
         df_metricas (pd.DataFrame): DataFrame contendo as métricas de avaliação para cada k.
    
     Returns:
         lista_caminhos (list): Lista com os caminhos completos das imagens a serem utilizadas no mosaico
         qtd_colunas (int): Quantidade de colunas para a geração do mosaico
         arquivo_gerar (str): Caminho completo da imagem a ser gerada com o mosaico
     
    """

    if len(lista_caminhos) == 0:
        raise ValueError("A lista de caminhos está vazia.")

    # 1. Abre todas as imagens
    imagens = [Image.open(caminho) for caminho in lista_caminhos]

    # 2. Usa a primeira imagem como referência de tamanho
    largura, altura = imagens[0].size

    # 3. Redimensiona todas para garantir alinhamento perfeito
    imagens = [
        img.resize((largura, altura)) if img.size != (largura, altura) else img
        for img in imagens
    ]

    # 4. Define layout
    total_imgs = len(imagens)
    linhas = math.ceil(total_imgs / qtd_colunas)

    # 5. Cria o canvas final
    mosaico = Image.new(
        'RGB',
        (largura * qtd_colunas, altura * linhas)
    )

    # 6. Cola as imagens respeitando a ordem da lista
    for idx, img in enumerate(imagens):
        linha = idx // qtd_colunas
        coluna = idx % qtd_colunas

        x = coluna * largura
        y = linha * altura

        mosaico.paste(img, (x, y))

    # 7. Salva o arquivo final
    mosaico.save(arquivo_gerar, subsampling=0, quality=100)

    log(f"✅ Mosaico gerado com sucesso ({qtd_colunas} colunas x {linhas} linhas): {arquivo_gerar}")