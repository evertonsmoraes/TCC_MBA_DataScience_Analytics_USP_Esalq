# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Created on 2025-08-29 17:10
Last Updated on 2026-04-11 11:15

@author: Everton Moraes

Autor: Everton Moraes
Data de criação: 29/08/2025
Última atualização: [preencher se necessário]
Versão: 1.0


Projeto: Clusterização de Municípios Brasileiros com Base em Indicadores Socioeconômicos Públicos

Descrição:
Este script tem como objetivo realizar a coleta, tratamento e estruturação de dados
socioeconômicos públicos provenientes da API do IBGE, com foco na obtenção de
indicadores em nível municipal (N6). O processo contempla a consulta de agregadores,
extração de metadados, filtragem por nível territorial e consolidação de indicadores
para análises exploratórias (discovery).

Etapas principais:
1. Consulta de agregadores por período (ex: 2022)
2. Extração e consolidação de metadados dos agregadores
3. Filtragem de agregadores com nível territorial municipal (N6) (ex: 3530607 - Mogi das Cruzes, SP)
4. Estruturação das combinações de variáveis, classificações e categorias
5. Coleta dos indicadores via API do IBGE
6. Consolidação dos dados em DataFrame único
7. Exportação dos dados para arquivos CSV (backup e saída final)

Dependências:
- pandas
- módulo customizado: funcoes_tcc

Entradas:
- Parâmetros definidos no script (ex: período, localidade)
- Dados obtidos via API do IBGE

Saídas:
- Arquivos CSV contendo:
    • Agregadores por período
    • Metadados dos agregadores
    • Agregadores filtrados (N6)
    • Base consolidada de indicadores (discovery)

Observações:
- O script utiliza checkpoints em arquivos CSV para evitar reprocessamento
- O controle de execução e logs é realizado via funções do módulo funcoes_tcc
- A localidade padrão pode ser ajustada conforme necessidade

"""
#%% BIBLIOTECAS E PARAMETROS GERAIS

import funcoes_tcc as fnc_tcc
import pandas as pd
from pathlib import Path

# Parâmetros Globais
diretorio = Path(__file__).resolve().parent # diretorio deste arquivo .py
diretorio_arquivos =  str(diretorio / "arquivos")
diretorio_imagens =  str(diretorio / "imgs")
        
#%% CONSULTA OS AGREGADORES POR PERIODO E SEUS METADADOS

# Consulta agregadores que contenham o periodo '2022'
fnc_tcc.log("Obtendo os agregadores pelo periodo")
df_agregador_periodo = fnc_tcc.consultar_conjunto_agregado('periodo','2022')
df_agregador_periodo['ID_AGREGADO'] = df_agregador_periodo['ID_AGREGADO'].astype('Int64')
fnc_tcc.log(f"Total registros Agregadores por Periodo: {df_agregador_periodo.shape[0]:,} registros")

# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv(df_agregador_periodo,diretorio_arquivos+"/rotina","df_agregador_periodo.csv")

# Check do df_agregador_periodo
print(df_agregador_periodo.info())

# Estrutura para Loop
dfs=[]
qtd_total_agregados = len(df_agregador_periodo)
cont_agred = 0

# Loop - Consultar os metadados dos agregados que contenham o perido '2022'
for row in df_agregador_periodo.itertuples(index=False): 
    cont_agred +=1
    # Obtem os metadados
    fnc_tcc.log(f"({cont_agred:,} de {qtd_total_agregados:,}) Obtendo Metadados do Id_Agregado: {row.ID_AGREGADO}")
    df_metadados_atual = fnc_tcc.consultar_metadados_agregado(row.ID_AGREGADO)
    # Empillha
    dfs.append(df_metadados_atual)

# Gerando Dataframe de metadados    
df_metadados_agregados = pd.concat(dfs, ignore_index=True)

# Incluido detalhes da pequisa
df_metadados_agregados = df_metadados_agregados.merge(df_agregador_periodo, on='ID_AGREGADO', how="left")
df_metadados_agregados.rename(columns={'AGREGADO_NOME_x': 'AGREGADO_NOME'}, inplace=True)

# Excluindo coluna duplicata
cols_y = [col for col in df_metadados_agregados.columns if col.endswith('_y')]
df_metadados_agregados.drop(columns=cols_y, inplace=True)

# Reorganizando as colunas
colunas = ['PERIODO']+['ID_PESQUISA']+ ['PESQUISA_NOME']+['ID_AGREGADO']+['AGREGADO_NOME'] + [col for col in df_metadados_agregados.columns  if col != 'PERIODO' and col != 'ID_PESQUISA' and col != 'PESQUISA_NOME' and col != 'ID_AGREGADO' and col != 'AGREGADO_NOME']
df_metadados_agregados = df_metadados_agregados[colunas]
fnc_tcc.log(f"Gerado DataFrame de Metadados(df_metadados_agregados) com {len(df_metadados_agregados):,} registros")

# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv(df_metadados_agregados,diretorio_arquivos+"/rotina","df_metadados_agregados.csv")

# Check do df_metadados_agregados
print(df_metadados_agregados.info())
print(df_metadados_agregados.shape)

# Verifica se existe o nivel de localidade 'N6' (Municipios)
df_metadados_agregados['TERRITORIO_NIVEL_DETALHE'].value_counts()

# Obtem os agregadores que contenham o nivel de localidade 'N6' (Municipios)
df_agregados_n6 = df_metadados_agregados[df_metadados_agregados["TERRITORIO_NIVEL_DETALHE"] == "N6"].copy()

# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv(df_agregados_n6,diretorio_arquivos+"/rotina","df_agregados_n6.csv")

# Check do df_agregados_n6
print(df_agregados_n6.info())
print(df_agregados_n6.shape)

#%% OBTEM OS INDICADORES PARA DISCOVERY

localidade = "N6[3530607]"  # Mogi das Cruzes

# Se não localizar o df_agregador_periodo, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_dataframe('df_agregador_periodo', globals()):
    fnc_tcc.log("DataFrame df_agregador_periodo não existente, iniciando importação do backup")
    chek_arq, df_agregador_periodo = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_agregador_periodo.csv",True,True)

# Se não localizar o df_metadados_agregados, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_dataframe('df_metadados_agregados', globals()):
    fnc_tcc.log("DataFrame df_metadados_agregados não existente, iniciando importação do backup")
    chek_arq, df_metadados_agregados = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_metadados_agregados.csv",True,True)

# Se não localizar o df_agregados_n6, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_dataframe('df_agregados_n6', globals()):
    fnc_tcc.log("DataFrame df_agregados_n6 não existente, iniciando importação do backup")
    chek_arq, df_agregados_n6 = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_agregados_n6.csv",True,True)

# Copia somente as colunas que serão utilizadas no loop
df_n6_simplificado = df_agregados_n6 [[
                                "PERIODO"
                                ,"ID_PESQUISA" 
                                ,"PESQUISA_NOME"
                                ,"ID_AGREGADO"
                                ,"AGREGADO_NOME"
                                ,"AGREGADO_ASSUNTO"
                                ,"AGREGADO_URL"
                                ,"VARIAVEL_ID"
                                ,"CLASSIFICACAO_ID"
                                ,"CATEGORIA_ID"]].copy()

# Garanta que não exista duplicidade nas combinações
df_n6_simplificado = df_n6_simplificado.drop_duplicates()

# df_n6_simplificado =  df_n6_simplificado.head(10).copy()
# print(df_n6_simplificado.info())
# print(df_n6_simplificado.shape)
# print(df_n6_simplificado.head)


# Gera DataFrame a consolidar os indicadores/valores
df_indicadores = pd.DataFrame(columns=["ID_PESQUISA"
                                       ,"PESQUISA_NOME"
                                       ,"ID_AGREGADO"
                                       ,"AGREGADO_NOME"
                                       ,"AGREGADO_ASSUNTO"
                                       ,"AGREGADO_URL"
                                       ,"ID_VARIAVEL"
                                       ,"VARIAVEL_NOME"
                                       ,"VAR_UNIDADE"
                                       ,"ID_CLASSIFICACAO"
                                       ,"CLASSIFICACAO_NOME"
                                       ,"ID_CATEGORIA"
                                       ,"CATEGORIA_NOME"
                                       ,"ID_NIVEL_LOCALIDADE"
                                       ,"NIVEL_LOCALIDADE_NOME"
                                       ,"ID_LOCALIDADE"
                                       ,"NOME_LOCALIDADE"
                                       ,"PERIODO"
                                       ,"VALOR"
                                       ])

# Parametros para o Loop
tt_indicadores = len(df_n6_simplificado)
cont = 0

# Loop nas combinacoes existentes
for linha in df_n6_simplificado.itertuples(index=False):

    cont +=1
    
    periodo = str(linha.PERIODO)
    str_id_agregado = str(linha.ID_AGREGADO)
    str_variavel_id = str(linha.VARIAVEL_ID)
    if str(linha.CLASSIFICACAO_ID) == "nan" or str(linha.CLASSIFICACAO_ID) == "<NA>":
        str_classificacao_id = None
    else:
        str_classificacao_id = str(linha.CLASSIFICACAO_ID).replace('.0','')
    str_categoria_id = str(linha.CATEGORIA_ID).replace('.0','').replace('nan','all').replace('<NA>','all')
    
    if str_classificacao_id:
        str_class_categ = f"{str_classificacao_id}[{str_categoria_id}]"
    else:
        str_class_categ = None
    
    fnc_tcc.log(f"({cont:,} de {tt_indicadores:,}) Obtendo indicadores/valores - ID_AGREGADOR: {str_id_agregado}, ID_VARIAVEL: {str_variavel_id}, CLASSIFICACAO: {str_class_categ}, LOCALIDADE: {localidade}")
    sucesso_obter_indicador, msg_erro, df_ret_indicador = fnc_tcc.obter_indicadores(
                                                                        str_id_agregado
                                                                        ,periodo
                                                                        ,str_variavel_id
                                                                        ,localidade
                                                                        ,str_class_categ
                                                                        )
    # Se obtidos os indicaodres, acrescenta colunas predeterminadas                                                                    
    if sucesso_obter_indicador:
           df_ret_indicador = df_ret_indicador.assign(
                                               ID_PESQUISA = linha.ID_PESQUISA
                                               ,PESQUISA_NOME = linha.PESQUISA_NOME
                                               ,ID_AGREGADO = linha.ID_AGREGADO
                                               ,AGREGADO_NOME = linha.AGREGADO_NOME
                                               ,AGREGADO_ASSUNTO = linha.AGREGADO_ASSUNTO
                                               ,AGREGADO_URL = linha.AGREGADO_URL
                                               )
           
           # Empilha o retorno do indicador
           df_indicadores = pd.concat([df_indicadores
                                       , df_ret_indicador]
                                       ,ignore_index=True)
    else:
        fnc_tcc.log(f"❌ Erro ao Obter o indicador/valor: {msg_erro}")
    


# Exporta os dados obtidos
nome_arquivo = df_indicadores["ID_LOCALIDADE"].iloc[0] + '_' + fnc_tcc.tratar_nome_arquivo(df_indicadores["NOME_LOCALIDADE"].iloc[0]).replace('(','').replace(')','')
fnc_tcc.exportar_dataframe_csv (df_indicadores,diretorio_arquivos+"/saida",f"00_discovery_dados_{nome_arquivo}.csv")




