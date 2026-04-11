# -*- coding: utf-8 -*-
"""
Created on 2025-09-19 20:04
Last Updated on 2026-04-11 11:15

@author: evert

Projeto: Clusterização de Municípios Brasileiros com Base em Indicadores Socioeconômicos Públicos

Módulo: Tratamento e Consolidação de Indicadores Municipais

Descrição:
Este script é responsável pela coleta, consolidação, tratamento e transformação
de indicadores socioeconômicos dos municípios brasileiros, utilizando dados
provenientes da API do IBGE.

O fluxo contempla a leitura de indicadores previamente selecionados, a extração
de dados para todos os municípios (nível N6), a estruturação de variáveis analíticas,
a criação de uma biblioteca padronizada de indicadores e a preparação dos dados
para análises estatísticas e modelos de clusterização.

Principais etapas:
1. Importação dos indicadores selecionados
2. Consulta dos dados via API do IBGE para todos os municípios
3. Consolidação dos dados em DataFrame único (df_dados_municipios)
4. Geração de arquivos individuais por município (opcional)
5. Construção da biblioteca de indicadores (metadados e regras de negócio)
6. Criação de variáveis derivadas (ex: taxas por 1.000 habitantes)
7. Padronização e organização das variáveis analíticas
8. Preparação dos dados para análises estatísticas e modelagem

Dependências:
- pandas
- sklearn (SimpleImputer, Pipeline, StandardScaler)
- pingouin
- matplotlib
- seaborn
- módulo customizado: funcoes_tcc

Entradas:
- Arquivo CSV contendo os indicadores selecionados
    (diretório: /entradas/indicadores_selecionados.csv)
- Dados obtidos via API do IBGE

Saídas:
- df_dados_municipios.csv → base consolidada com indicadores brutos
- df_parametros_indicadores_selecionados.csv → parâmetros utilizados na coleta
- Arquivos por município (opcional) → detalhamento individual
- df_biblioteca_indicadores.csv → catálogo de indicadores
- df_biblioteca.csv → catálogo completo com indicadores e critério de rankings

Estrutura dos dados:
- Cada linha representa um indicador para um município em determinado período
- Variáveis derivadas são criadas para padronização e análise comparativa
- Indicadores auxiliares são utilizados para cálculo de métricas compostas

Regras de negócio relevantes:
- Tratamento de valores nulos e inconsistentes
- Conversão de tipos (string → numérico)
- Cálculo de indicadores normalizados (ex: por 1.000 habitantes)
- Padronização de classificações e categorias da API IBGE
- Controle de duplicidade e integridade dos dados

Observações:
- O script utiliza checkpoints em CSV para evitar reprocessamento
- Logs de execução são controlados via funcoes_tcc.log()
- Pode demandar alto tempo de execução devido ao volume de requisições na API
- Recomenda-se execução em ambiente com boa conectividade


"""
#%% BIBLIOTECAS E PARAMETROS GERAIS

# !pip install pingouin
import funcoes_tcc as fnc_tcc
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import pingouin as pg
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Parâmetros Globais
diretorio = Path(__file__).resolve().parent # diretorio deste arquivo .py
diretorio_arquivos =  str(diretorio / "arquivos")
diretorio_imagens =  str(diretorio / "imgs")

#%% OBTEM OS INDICADORES

# Importar os indicadores selecionados
chek_arq, df_indicadores_selecionados = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/entradas","indicadores_selecionados.csv",True,True)

"""
df_indicadores_selecionados = df_indicadores_selecionados[
    (df_indicadores_selecionados['ID_AGREGADO'] == 10089) &
    (df_indicadores_selecionados['ID_VARIAVEL'] == 1000093) &
    (df_indicadores_selecionados['ID_CLASSIFICACAO'] == "1 | 2 | 58 | 2661") &
    (df_indicadores_selecionados['ID_CATEGORIA'] == "1 | 6794 | 95253 | 32776")
].copy()
"""

# Gera DataFrame que consolida os parametros do indicadores obtidos
df_parametros_indicadores_selecionados = pd.DataFrame({
                                    "ID_PESQUISA": pd.Series(dtype="string")
                                    ,"PESQUISA_NOME": pd.Series(dtype="string")
                                    ,"ID_AGREGADO": pd.Series(dtype="int64")
                                    ,"AGREGADO_NOME": pd.Series(dtype="string")
                                    ,"AGREGADO_ASSUNTO": pd.Series(dtype="string")
                                    ,"PERIODO": pd.Series(dtype="string")
                                    ,"ID_VARIAVEL": pd.Series(dtype="int64")
                                    ,"VARIAVEL_NOME": pd.Series(dtype="string")
                                    ,"VAR_UNIDADE": pd.Series(dtype="string")
                                    ,"LOCALIDADE": pd.Series(dtype="string")
                                    ,"CLASSIFICACAO": pd.Series(dtype="string")
                                    ,"DESCRICAO_CONSOLIDADA": pd.Series(dtype="string")
                                    })

# Gera DataFrame a consolidar os indicadores/valores
df_dados_municipios = pd.DataFrame({
                                    "ID_PESQUISA": pd.Series(dtype="string")
                                    ,"PESQUISA_NOME": pd.Series(dtype="string")
                                    ,"ID_AGREGADO": pd.Series(dtype="int64")
                                    ,"AGREGADO_NOME": pd.Series(dtype="string")
                                    ,"AGREGADO_ASSUNTO": pd.Series(dtype="string")
                                    ,"AGREGADO_URL": pd.Series(dtype="string")
                                    ,"ID_VARIAVEL": pd.Series(dtype="int64")
                                    ,"VARIAVEL_NOME": pd.Series(dtype="string")
                                    ,"VAR_UNIDADE": pd.Series(dtype="string")
                                    ,"ID_CLASSIFICACAO": pd.Series(dtype="string")
                                    ,"CLASSIFICACAO_NOME": pd.Series(dtype="string")
                                    ,"ID_CATEGORIA": pd.Series(dtype="string")
                                    ,"CATEGORIA_NOME": pd.Series(dtype="string")
                                    ,"ID_NIVEL_LOCALIDADE": pd.Series(dtype="string")
                                    ,"NIVEL_LOCALIDADE_NOME": pd.Series(dtype="string")
                                    ,"ID_LOCALIDADE": pd.Series(dtype="int64")
                                    ,"NOME_LOCALIDADE": pd.Series(dtype="string")
                                    ,"PERIODO": pd.Series(dtype="string")
                                    ,"VALOR": pd.Series(dtype="string")
                                    })

# Parametros para o Loop
tt_indicadores = len(df_indicadores_selecionados)
cont = 0
localidade = 'N6[all]' # Obter todos os municipios simultaneamente


# Loop nas combinacoes existentes
for linha in df_indicadores_selecionados.itertuples(index=False):

    cont +=1
    
    periodo = str(linha.PERIODO)
    str_id_agregado = str(linha.ID_AGREGADO)
    str_variavel_id = str(linha.ID_VARIAVEL)
    
    # Se tiver valor no ID_CLASSIFICACAO e for diferente de [None]
    if linha.ID_CLASSIFICACAO and linha.ID_CLASSIFICACAO != "[None]":
    
        # Verifica se a Classificacao tem mais de um valor
        if "|" in str(linha.ID_CLASSIFICACAO): 
            
            lista_class = [x.strip() for x in str(linha.ID_CLASSIFICACAO).split('|')]
            lista_cat = [x.strip() for x in str(linha.ID_CATEGORIA).split('|')]
    
            if len(lista_class) != len(lista_cat):
                raise ValueError("❌ Erro: Quantidade de Classificação e Categorias estão diferentes")
            
            str_class_categ = "|".join(f"{a}[{b}]" for a, b in zip(lista_class, lista_cat))
            
        # Se não tiver trata como se tivesse um único valor
        else:
            
            # Tratativa Classificacao e Categoria
            if str(linha.ID_CLASSIFICACAO) == "nan" or str(linha.ID_CLASSIFICACAO) == "<NA>":
                str_classificacao_id = None
            else:
                str_classificacao_id = str(linha.ID_CLASSIFICACAO).replace('.0','')
            str_categoria_id = str(linha.ID_CATEGORIA).replace('.0','').replace('nan','all').replace('<NA>','all')
            
            if str_classificacao_id:
                str_class_categ = f"{str_classificacao_id}[{str_categoria_id}]"
            else:
                str_class_categ = None
     
    # Se não tiver valor no ID_CLASSIFICACAO
    else:
        str_class_categ = None
    
    # Obtendo os indicadores
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
           
           
           # Empilha os parametros do indicador
           df_parametros_indicadores_selecionados = pd.concat([
                                                                df_parametros_indicadores_selecionados
                                                                ,pd.DataFrame([{
                                                                    "ID_PESQUISA": linha.ID_PESQUISA
                                                                    ,"PESQUISA_NOME": linha.PESQUISA_NOME
                                                                    ,"ID_AGREGADO": str_id_agregado
                                                                    ,"AGREGADO_NOME": linha.AGREGADO_NOME
                                                                    ,"AGREGADO_ASSUNTO" : linha.AGREGADO_ASSUNTO
                                                                    ,"PERIODO": periodo
                                                                    ,"ID_VARIAVEL": str_variavel_id
                                                                    ,"VARIAVEL_NOME": linha.VARIAVEL_NOME
                                                                    ,"VAR_UNIDADE": linha.VAR_UNIDADE
                                                                    ,"LOCALIDADE": localidade
                                                                    ,"CLASSIFICACAO": str_class_categ
                                                                    ,"DESCRICAO_CONSOLIDADA": f"CLASSIFICACAO_NOME: {linha.CLASSIFICACAO_NOME} / CATEGORIA_NOME: {linha.CATEGORIA_NOME}"
                                                                }])
                                                            ],
                                                            ignore_index=True
                                                        )
           # Empilha o retorno do indicador
           df_dados_municipios = pd.concat([df_dados_municipios
                                       , df_ret_indicador]
                                       ,ignore_index=True)
    else:
        fnc_tcc.log(f"❌ Erro ao Obter o indicador/valor: {msg_erro}")
    
# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv (df_dados_municipios,diretorio_arquivos+"/rotina","df_dados_municipios.csv")  
fnc_tcc.exportar_dataframe_csv (df_parametros_indicadores_selecionados,diretorio_arquivos+"/rotina","df_parametros_indicadores_selecionados.csv")  

fnc_tcc.log(f"Total de municipios: {df_dados_municipios['ID_LOCALIDADE'].nunique():,} municipios")
fnc_tcc.log(f"Total registros dos municipios: {df_dados_municipios.shape[0]:,} registros")

   
### EXPORTAR ARQUIVOS POR MUNICIPIO

# Listar Municipios 
df_municipios_brasileiros = df_dados_municipios [[
                                "ID_LOCALIDADE"
                                ,"NOME_LOCALIDADE" 
                                ]].drop_duplicates().copy()

cont=0
tt_municipios = df_municipios_brasileiros['ID_LOCALIDADE'].nunique()


# Loop dos Municipios
for municipio_atual in df_municipios_brasileiros.itertuples(index=False):
 
    cont+=1
    nome_arquivo = str(municipio_atual.ID_LOCALIDADE)+"_"+fnc_tcc.tratar_nome_arquivo(municipio_atual.NOME_LOCALIDADE.replace(' - ',' '))+".csv"
    
    fnc_tcc.log(f"({cont:,} de {tt_municipios:,}) - Gerando arquivo do município: {municipio_atual.ID_LOCALIDADE} - {municipio_atual.NOME_LOCALIDADE}")
    
    # Separar os Indicadores
    df_municipio_atual = df_dados_municipios[df_dados_municipios["ID_LOCALIDADE"] == municipio_atual.ID_LOCALIDADE]
    
    # Exportar os dados
    fnc_tcc.exportar_dataframe_csv (df_municipio_atual,diretorio_arquivos+"/saida",nome_arquivo)  


# df_mogi = df_dados_municipios[df_dados_municipios["ID_LOCALIDADE"] == 3530607]
# df_mogi

#%% BIBLIOTECA DE INDICADORES

df_biblioteca_indicadores = pd.DataFrame([
    
    #API IBGE
    {"REFERENCIA_INDICADOR": "VAR_01","GRUPO_INDICADOR": "Dados Gerais","DESCRICAO_VARIAVEL": "Área Territorial","UNIDADE_MEDIDA": "km²","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_02","GRUPO_INDICADOR": "Dados Gerais","DESCRICAO_VARIAVEL": "Total população","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_03","GRUPO_INDICADOR": "Dados Gerais","DESCRICAO_VARIAVEL": "Densidade demográfica","UNIDADE_MEDIDA": "habitante por km³","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_04","GRUPO_INDICADOR": "População","DESCRICAO_VARIAVEL": "% Homem","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_05","GRUPO_INDICADOR": "População","DESCRICAO_VARIAVEL": "% Mulher","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_06","GRUPO_INDICADOR": "População","DESCRICAO_VARIAVEL": "Idade Mediana","UNIDADE_MEDIDA": "anos","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_07","GRUPO_INDICADOR": "População","DESCRICAO_VARIAVEL": "Índice de Envelhecimento","UNIDADE_MEDIDA": "razão","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_08","GRUPO_INDICADOR": "População","DESCRICAO_VARIAVEL": "% População Urbana","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_09","GRUPO_INDICADOR": "População","DESCRICAO_VARIAVEL": "% População Rural","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_10","GRUPO_INDICADOR": "Educação","DESCRICAO_VARIAVEL": "Taxa de escolarização de 6 a 14 anos de idade","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_11","GRUPO_INDICADOR": "Educação","DESCRICAO_VARIAVEL": "Número médio de anos de estudo das pessoas de 11 anos ou mais de idade","UNIDADE_MEDIDA": "anos","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_12","GRUPO_INDICADOR": "Educação","DESCRICAO_VARIAVEL": "Quantidade de Empresas e outras organizaçõe de Educação Infantil e Ensino Fundamental","UNIDADE_MEDIDA": "unidades por 1.000 habitantes","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_12_1","GRUPO_INDICADOR": "[Auxiliar] Educação","DESCRICAO_VARIAVEL": "[Auxiliar] Quantidade de Empresas e outras organizaçõe de Educação Infantil e Ensino Fundamental","UNIDADE_MEDIDA": "unidade","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_12_2","GRUPO_INDICADOR": "[Auxiliar] Educação","DESCRICAO_VARIAVEL": "[Auxiliar] Total população","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_13","GRUPO_INDICADOR": "Educação","DESCRICAO_VARIAVEL": "Quantidade de Empresas e outras organizaçõe de Ensino Médio","UNIDADE_MEDIDA": "unidades por 1.000 habitantes","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_13_1","GRUPO_INDICADOR": "[Auxiliar] Educação","DESCRICAO_VARIAVEL": "[Auxiliar] Quantidade de Empresas e outras organizaçõe de Ensino Médio","UNIDADE_MEDIDA": "unidade","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_13_2","GRUPO_INDICADOR": "[Auxiliar] Educação","DESCRICAO_VARIAVEL": "[Auxiliar] Total população","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_14","GRUPO_INDICADOR": "Educação","DESCRICAO_VARIAVEL": "Quantidade de Empresas e outras organizaçõe de Ensino Superior","UNIDADE_MEDIDA": "unidades por 1.000 habitantes","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_14_1","GRUPO_INDICADOR": "[Auxiliar] Educação","DESCRICAO_VARIAVEL": "[Auxiliar] Quantidade de Empresas e outras organizaçõe de Ensino Superior","UNIDADE_MEDIDA": "unidade","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_14_2","GRUPO_INDICADOR": "[Auxiliar] Educação","DESCRICAO_VARIAVEL": "[Auxiliar] Total população","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_15","GRUPO_INDICADOR": "Saúde","DESCRICAO_VARIAVEL": "Taxa de Natalidade","UNIDADE_MEDIDA": "nascidos vivos por 1.000 habitantes","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_15_1","GRUPO_INDICADOR": "[Auxiliar] Saúde","DESCRICAO_VARIAVEL": "[Auxiliar] Nascidos Vivos","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_15_2","GRUPO_INDICADOR": "[Auxiliar] Saúde","DESCRICAO_VARIAVEL": "[Auxiliar] Total população","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_16","GRUPO_INDICADOR": "Saúde","DESCRICAO_VARIAVEL": "Taxa de óbito crianças","UNIDADE_MEDIDA": "óbitos de crianças até 1 ano por 1.000 nascidos vivos","CRITERIO_RANK": "crescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_16_1","GRUPO_INDICADOR": "[Auxiliar] Saúde","DESCRICAO_VARIAVEL": "[Auxiliar] Óbitos com idade menos de 1 ano","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "crescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_16_2","GRUPO_INDICADOR": "[Auxiliar] Saúde","DESCRICAO_VARIAVEL": "[Auxiliar] Nascidos Vivos","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_17","GRUPO_INDICADOR": "Saúde","DESCRICAO_VARIAVEL": "Quantidade de hospitais por mil habitantes","UNIDADE_MEDIDA": "unidades por 1.000 habitantes","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_17_1","GRUPO_INDICADOR": "[Auxiliar] Saúde","DESCRICAO_VARIAVEL": "[Auxiliar] Quantidade de hospitais","UNIDADE_MEDIDA": "unidade","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_17_2","GRUPO_INDICADOR": "[Auxiliar] Saúde","DESCRICAO_VARIAVEL": "[Auxiliar] Total população","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_18","GRUPO_INDICADOR": "Economia","DESCRICAO_VARIAVEL": "Salário médio mensal dos trabalhadores formais","UNIDADE_MEDIDA": "salários mínimos","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_19","GRUPO_INDICADOR": "Economia","DESCRICAO_VARIAVEL": "Salário médio mensal dos trabalhadores formais","UNIDADE_MEDIDA": "reais","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_20","GRUPO_INDICADOR": "Economia","DESCRICAO_VARIAVEL": "Pessoal ocupado em postos de trabalho formais","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_21","GRUPO_INDICADOR": "Economia","DESCRICAO_VARIAVEL": "Taxa de Ocupação","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_21_1","GRUPO_INDICADOR": "[Auxiliar] Economia","DESCRICAO_VARIAVEL": "[Auxiliar] Pessoal ocupado em postos de trabalho formais","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_21_2","GRUPO_INDICADOR": "[Auxiliar] Economia","DESCRICAO_VARIAVEL": "[Auxiliar] População de 18 a 65 anos","UNIDADE_MEDIDA": "pessoas","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_22","GRUPO_INDICADOR": "Infraestrutura","DESCRICAO_VARIAVEL": "Taxa de domicílios com esgotamento sanitário por rede geral, rede pluvial ou fossa ligada à rede esgotamento sanitário por rede geral, rede pluvial ou fossa ligada à rede","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_23","GRUPO_INDICADOR": "Infraestrutura","DESCRICAO_VARIAVEL": "Taxa de domicílios com coleta de lixo","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}
    ,{"REFERENCIA_INDICADOR": "VAR_24","GRUPO_INDICADOR": "Infraestrutura","DESCRICAO_VARIAVEL": "Taxa de domicílios com internet","UNIDADE_MEDIDA": "%","CRITERIO_RANK": "descrescente","ORIGEM": "API IBGE","PERIODO": "2022"}

])

# Gera lista das variaveis, excluindo as auxiliares
ls_variaveis = (
    df_biblioteca_indicadores['REFERENCIA_INDICADOR']
    .dropna()
    .unique()
    .tolist()
)


# Gerando Bliblioteca dos rank
df_biblioteca_rank = pd.DataFrame({
        "REFERENCIA_INDICADOR": "RANK_" + df_biblioteca_indicadores["REFERENCIA_INDICADOR"]
        ,"GRUPO_INDICADOR": "[Rank] " + df_biblioteca_indicadores["GRUPO_INDICADOR"]
        ,"DESCRICAO_VARIAVEL": "Ranking " + df_biblioteca_indicadores["CRITERIO_RANK"] + " da variável "+ df_biblioteca_indicadores["DESCRICAO_VARIAVEL"]
        ,"UNIDADE_MEDIDA": "º"
        ,"CRITERIO_RANK": df_biblioteca_indicadores["CRITERIO_RANK"]
})


# Empilha Biblioteca do Rank com dos indicaodres
df_biblioteca = pd.concat([df_biblioteca_indicadores,df_biblioteca_rank], axis=0)

# Mapa de Abreviacoes
mapa_abreviacoes_grupos = {
    "Dados Gerais": "geral"
    ,"População": 'popul'
    ,"Educação": 'educ'
    ,"Saúde": 'saud'
    ,"Economia": 'ecom'
    ,"Infraestrutura": 'infra'
   
}


# Mapara de casas decimais
mapa_casas_decimais = {
     "pessoas": 0,
     "unidade": 0,
     "º": 0,
     "anos": 1,
     "salários mínimos": 1,
     "habitante por km³": 2,
     "%": 2,
     "razão": 2,
     "unidades por 1.000 habitantes":2,
     "nascidos vivos por 1.000 habitantes": 2,
     "óbitos de crianças até 1 ano por 1.000 nascidos vivos": 2,
     "reais": 2,
     "km²": 3,
 }


# Mapa de descrição da varíavel
mapa_descricao_var = (
    df_biblioteca
    .set_index("REFERENCIA_INDICADOR")["DESCRICAO_VARIAVEL"]
    .to_dict()
)

# Mapa de descrição da varíavel
mapa_grupo_var = (
    df_biblioteca
    .set_index("REFERENCIA_INDICADOR")["GRUPO_INDICADOR"]
    .to_dict()
)

# Mapa de descrição da varíavel
mapa_unidade_var = (
    df_biblioteca
    .set_index("REFERENCIA_INDICADOR")["UNIDADE_MEDIDA"]
    .to_dict()
)


# Mapa de prioridade rank
mapa_criterio_rank = (
    df_biblioteca
    .set_index("REFERENCIA_INDICADOR")["CRITERIO_RANK"]
    .to_dict()
)


fnc_tcc.log(f"Total de indicadores cadastrados: {df_biblioteca_indicadores.shape[0]:,} indicadores")
# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv (df_biblioteca_indicadores,diretorio_arquivos+"/rotina","df_biblioteca_indicadores.csv")  


fnc_tcc.log(f"Total de registros na biblioteca: {df_biblioteca.shape[0]:,} registros")
# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv (df_biblioteca,diretorio_arquivos+"/rotina","df_biblioteca.csv")  

#%% TRATAR INDICADORES POR MUNICIPIO

# Se não localizar o df_dados_municipios, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_dados_municipios', globals()):
    fnc_tcc.log("DataFrame df_dados_municipios não existente, iniciando importação do backup")
    chek_arq, df_dados_municipios = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_dados_municipios.csv",True,True)


# Se não localizar o df_biblioteca, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_biblioteca', globals()):
    fnc_tcc.log("DataFrame df_biblioteca não existente, iniciando importação do backup")
    chek_arq, df_biblioteca = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_biblioteca.csv",True,True)


# lista que terá os indicadores empilhados
ls_variaveis_municipios = []

# Garante os formatos para gerar as variaveis
df_dados_municipios["ID_AGREGADO"] = df_dados_municipios["ID_AGREGADO"].astype(str)
df_dados_municipios["ID_VARIAVEL"] = df_dados_municipios["ID_VARIAVEL"].astype(str)

# Filtra Mogi das Cruzes
#df_dados_municipios = df_dados_municipios[df_dados_municipios["ID_LOCALIDADE"] == 3530607]
#df_dados_municipios.info()

cont=0
tt_municipios = df_dados_municipios['ID_LOCALIDADE'].nunique()
fnc_tcc.log(f"Total de municipios a tratar as variaveis: {tt_municipios:,} municípios")

# Loop nas localidades
for id_local in df_dados_municipios["ID_LOCALIDADE"].unique():
   
    df_local = df_dados_municipios[df_dados_municipios["ID_LOCALIDADE"] == id_local]

    cont+=1
    fnc_tcc.log(f"({cont:,} de {tt_municipios:,}) - Tratando a variáveis do município: {df_local['ID_LOCALIDADE'].iloc[0]} - {df_local['NOME_LOCALIDADE'].iloc[0]}")
    ls_variaveis_municipios.append({
        
        # ID_LOCALIDADE
        "ID_LOCALIDADE": df_local["ID_LOCALIDADE"].iloc[0]
        
        # NOME_LOCALIDADE
        ,"NOME_LOCALIDADE": df_local["NOME_LOCALIDADE"].iloc[0]

        # Área Territorial
        ,"VAR_01": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "6318")
                    ,"VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "6318")
                    ,"VALOR"].empty else None

        # Total população
        ,"VAR_02": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None

        # Densidade demográfica
        ,"VAR_03": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "614")
                    , "VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "614")
                    ,"VALOR"].empty else None

        # % Homem
        ,"VAR_04": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "9606")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "2 | 86 | 287")
                    & (df_local["ID_CATEGORIA"] == "4 | 95251 | 100362")
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9606")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "2 | 86 | 287")
                    & (df_local["ID_CATEGORIA"] == "4 | 95251 | 100362")
                    ,"VALOR"].empty else None
                
        # % Mulher
        ,"VAR_05": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "9606")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "2 | 86 | 287")
                    & (df_local["ID_CATEGORIA"] == "5 | 95251 | 100362")
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9606")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "2 | 86 | 287")
                    & (df_local["ID_CATEGORIA"] == "5 | 95251 | 100362")
                    ,"VALOR"].empty else None
        
        # Idade Mediana
        ,"VAR_06": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "9756")
                    & (df_local["ID_VARIAVEL"] == "10613")
                    & (df_local["ID_CLASSIFICACAO"] == "86")
                    & (df_local["ID_CATEGORIA"] == "95251")
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9756")
                    & (df_local["ID_VARIAVEL"] == "10613")
                    & (df_local["ID_CLASSIFICACAO"] == "86")
                    & (df_local["ID_CATEGORIA"] == "95251")
                    ,"VALOR"].empty else None

        # Índice de Envelhecimento
        ,"VAR_07": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "9515")
                    & (df_local["ID_VARIAVEL"] == "10612")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9515")
                    & (df_local["ID_VARIAVEL"] == "10612")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
      
        # % População Urbana
        ,"VAR_08": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "10089")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "1 | 2 | 58 | 2661")
                    & (df_local["ID_CATEGORIA"] == "1 | 6794 | 95253 | 32776")
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "10089")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "1 | 2 | 58 | 2661")
                    & (df_local["ID_CATEGORIA"] == "1 | 6794 | 95253 | 32776")
                    ,"VALOR"].empty else None

        # % População Rural
        ,"VAR_09": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "10089")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "1 | 2 | 58 | 2661")
                    & (df_local["ID_CATEGORIA"] == "2 | 6794 | 95253 | 32776")
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "10089")
                    & (df_local["ID_VARIAVEL"] == "1000093")
                    & (df_local["ID_CLASSIFICACAO"] == "1 | 2 | 58 | 2661")
                    & (df_local["ID_CATEGORIA"] == "2 | 6794 | 95253 | 32776")
                    ,"VALOR"].empty else None
        
        # Taxa de escolarização de 6 a 14 anos de idade
        ,"VAR_10": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "10150")
                    & (df_local["ID_VARIAVEL"] == "12805")
                    & (df_local["ID_CLASSIFICACAO"] == "58 | 2")
                    & (df_local["ID_CATEGORIA"] == "31615 | 6794")
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "10150")
                    & (df_local["ID_VARIAVEL"] == "12805")
                    & (df_local["ID_CLASSIFICACAO"] == "58 | 2")
                    & (df_local["ID_CATEGORIA"] == "31615 | 6794")
                    ,"VALOR"].empty else None
        
        # Número médio de anos de estudo das pessoas de 11 anos ou mais de idade
        ,"VAR_11": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "10062")
                    & (df_local["ID_VARIAVEL"] == "13285")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "10062")
                    & (df_local["ID_VARIAVEL"] == "13285")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
        
        
        # Quantidade de Empreesas Ensino Infantil e Fundamental - Numerador
        ,"VAR_12_1": pd.to_numeric(df_local.loc[
                     (df_local["ID_AGREGADO"] == "9418")
                     & (df_local["ID_VARIAVEL"] == "2585")
                     & (df_local["ID_CLASSIFICACAO"] == "12762")
                     & (df_local["ID_CATEGORIA"] == "117790")
                     ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9418")
                    & (df_local["ID_VARIAVEL"] == "2585")
                    & (df_local["ID_CLASSIFICACAO"] == "12762")
                    & (df_local["ID_CATEGORIA"] == "117790")
                    ,"VALOR"].empty else None
        
        
        # Total população - Denominador
        ,"VAR_12_2": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
        
        
        # Quantidade de Empreesas Ensino Infantil e Fundamental para cada 1.000 habitantes
        ,"VAR_12": ((
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "9418")
                        & (df_local["ID_VARIAVEL"] == "2585")
                        & (df_local["ID_CLASSIFICACAO"] == "12762")
                        & (df_local["ID_CATEGORIA"] == "117790")
                        ,"VALOR"], errors="coerce").iloc[0]  # Quantidade de Empreesas Ensino Infantil e Fundamental
                    /
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "4714")
                        & (df_local["ID_VARIAVEL"] == "93")
                        # ID_CLASSIFICACAO N/A
                        # ID_CATEGORIA N/A
                        ,"VALOR"], errors="coerce").iloc[0] # Total População
                    ) 
                    *1000 # por 1.000 habitantes 
                    if (
                        not df_local.loc[
                        (df_local["ID_AGREGADO"] == "9418")
                        & (df_local["ID_VARIAVEL"] == "2585")
                        & (df_local["ID_CLASSIFICACAO"] == "12762")
                        & (df_local["ID_CATEGORIA"] == "117790")
                        ,"VALOR"].empty  # Quantidade de Empreesas Ensino Infantil e Fundamental
                        and 
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "4714")
                         & (df_local["ID_VARIAVEL"] == "93")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Total População
                        
                    )
                    else None
        )
        
        # Quantidade de Empreesas Ensino Médio - Numerador
        ,"VAR_13_1": pd.to_numeric(df_local.loc[
                     (df_local["ID_AGREGADO"] == "9418")
                     & (df_local["ID_VARIAVEL"] == "2585")
                     & (df_local["ID_CLASSIFICACAO"] == "12762")
                     & (df_local["ID_CATEGORIA"] == "117794")
                     ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9418")
                    & (df_local["ID_VARIAVEL"] == "2585")
                    & (df_local["ID_CLASSIFICACAO"] == "12762")
                    & (df_local["ID_CATEGORIA"] == "117794")
                    ,"VALOR"].empty else None
        
        
        # Total população - Denominador
        ,"VAR_13_2": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
        
        
        # Quantidade de Empreesas Ensino Médio para cada 1.000 habitantes
        ,"VAR_13": ((
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "9418")
                        & (df_local["ID_VARIAVEL"] == "2585")
                        & (df_local["ID_CLASSIFICACAO"] == "12762")
                        & (df_local["ID_CATEGORIA"] == "117794")
                        ,"VALOR"], errors="coerce").iloc[0]  # Quantidade de Empreesas Ensino Médio
                    /
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "4714")
                        & (df_local["ID_VARIAVEL"] == "93")
                        # ID_CLASSIFICACAO N/A
                        # ID_CATEGORIA N/A
                        ,"VALOR"], errors="coerce").iloc[0] # Total População
                    ) 
                    *1000 # por 1.000 habitantes 
                    if (
                        not df_local.loc[
                        (df_local["ID_AGREGADO"] == "9418")
                        & (df_local["ID_VARIAVEL"] == "2585")
                        & (df_local["ID_CLASSIFICACAO"] == "12762")
                        & (df_local["ID_CATEGORIA"] == "117794")
                        ,"VALOR"].empty  # Quantidade de Empreesas Ensino Médio
                        and 
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "4714")
                         & (df_local["ID_VARIAVEL"] == "93")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Total População
                        
                    )
                    else None
        )
        
        
        # Quantidade de Empreesas Ensino Superior - Numerador
        ,"VAR_14_1": pd.to_numeric(df_local.loc[
                     (df_local["ID_AGREGADO"] == "9418")
                     & (df_local["ID_VARIAVEL"] == "2585")
                     & (df_local["ID_CLASSIFICACAO"] == "12762")
                     & (df_local["ID_CATEGORIA"] == "117796")
                     ,"VALOR"], errors="coerce").iloc[0]
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9418")
                    & (df_local["ID_VARIAVEL"] == "2585")
                    & (df_local["ID_CLASSIFICACAO"] == "12762")
                    & (df_local["ID_CATEGORIA"] == "117796")
                    ,"VALOR"].empty else None
        
        
        # Total população - Denominador
        ,"VAR_14_2": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
        
        
        # Quantidade de Empreesas Ensino Superior para cada 1.000 habitantes
        ,"VAR_14": ((
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "9418")
                        & (df_local["ID_VARIAVEL"] == "2585")
                        & (df_local["ID_CLASSIFICACAO"] == "12762")
                        & (df_local["ID_CATEGORIA"] == "117796")
                        ,"VALOR"], errors="coerce").iloc[0]  # Quantidade de Empreesas Ensino Superior
                    /
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "4714")
                        & (df_local["ID_VARIAVEL"] == "93")
                        # ID_CLASSIFICACAO N/A
                        # ID_CATEGORIA N/A
                        ,"VALOR"], errors="coerce").iloc[0] # Total População
                    ) 
                    *1000 # por 1.000 habitantes 
                    if (
                        not df_local.loc[
                        (df_local["ID_AGREGADO"] == "9418")
                        & (df_local["ID_VARIAVEL"] == "2585")
                        & (df_local["ID_CLASSIFICACAO"] == "12762")
                        & (df_local["ID_CATEGORIA"] == "117796")
                        ,"VALOR"].empty  # Quantidade de Empreesas Ensino Médio
                        and 
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "4714")
                         & (df_local["ID_VARIAVEL"] == "93")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Total População
                        
                    )
                    else None
        )
        
        

        
       # Taxa de Natalidade - Numerador - Nascidos Vivos
       ,"VAR_15_1": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "2609")
                    & (df_local["ID_VARIAVEL"] == "217")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
               if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "2609")
                    & (df_local["ID_VARIAVEL"] == "217")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
       
       # Taxa de Natalidade - Denominador - Soma da Total População
       ,"VAR_15_2": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "4714")
                   & (df_local["ID_VARIAVEL"] == "93")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR"], errors="coerce").iloc[0]
           if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "4714")
                   & (df_local["ID_VARIAVEL"] == "93")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR"].empty else None
                

        # Taxa de Natalidade
        ,"VAR_15": ((
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "2609")
                        & (df_local["ID_VARIAVEL"] == "217")
                        # ID_CLASSIFICACAO N/A
                        # ID_CATEGORIA N/A
                        ,"VALOR"], errors="coerce").iloc[0] # Nascidos Vivos
                    /
                    pd.to_numeric(df_local.loc[
                        (df_local["ID_AGREGADO"] == "4714")
                        & (df_local["ID_VARIAVEL"] == "93")
                        # ID_CLASSIFICACAO N/A
                        # ID_CATEGORIA N/A
                        ,"VALOR"], errors="coerce").iloc[0] # Total População
                    ) 
                    *1000 # por 1.000 habitantes 
                    if (
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "2609")
                         & (df_local["ID_VARIAVEL"] == "217")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Nascidos Vivos
                        and 
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "4714")
                         & (df_local["ID_VARIAVEL"] == "93")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Total População
                        
                    )
                    else None
        )
        
        # Taxa de óbito crianças - Numerador - Óbitos com idade menos de 1 ano
       ,"VAR_16_1": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "2681")
                   & (df_local["ID_VARIAVEL"] == "343")
                   & (df_local["ID_CLASSIFICACAO"] == "260 | 2 | 244 | 257 | 1836")
                   & (df_local["ID_CATEGORIA"] == "5922 | 0 | 0 | 0 | 0")
                   ,"VALOR"], errors="coerce").iloc[0]
                   
               if not  df_local.loc[
                     (df_local["ID_AGREGADO"] == "2681")
                     & (df_local["ID_VARIAVEL"] == "343")
                     & (df_local["ID_CLASSIFICACAO"] == "260 | 2 | 244 | 257 | 1836")
                     & (df_local["ID_CATEGORIA"] == "5922 | 0 | 0 | 0 | 0")
                     ,"VALOR"].empty else None
               
        # Taxa de óbito crianças - Denominador - Nascidos Vivos
        ,"VAR_16_2": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "2609")
                    & (df_local["ID_VARIAVEL"] == "217")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0] 
                if not df_local.loc[
                     (df_local["ID_AGREGADO"] == "2609")
                     & (df_local["ID_VARIAVEL"] == "217")
                     # ID_CLASSIFICACAO N/A
                     # ID_CATEGORIA N/A
                     ,"VALOR"].empty else None
                
        # Taxa de óbito crianças
        ,"VAR_16": ((
                    pd.to_numeric(df_local.loc[
                                (df_local["ID_AGREGADO"] == "2681")
                                & (df_local["ID_VARIAVEL"] == "343")
                                & (df_local["ID_CLASSIFICACAO"] == "260 | 2 | 244 | 257 | 1836")
                                & (df_local["ID_CATEGORIA"] == "5922 | 0 | 0 | 0 | 0")
                                ,"VALOR"], errors="coerce").iloc[0] # Óbitos com idade menos de 1 ano
                    /        
                    pd.to_numeric(df_local.loc[
                                (df_local["ID_AGREGADO"] == "2609")
                                & (df_local["ID_VARIAVEL"] == "217")
                                # ID_CLASSIFICACAO N/A
                                # ID_CATEGORIA N/A
                                ,"VALOR"], errors="coerce").iloc[0]  # Nascidos Vivos
                    ) 
                    *1000 # por 1.000 nascidos vivos 
                    if (
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "2681")
                         & (df_local["ID_VARIAVEL"] == "343")
                         & (df_local["ID_CLASSIFICACAO"] == "260 | 2 | 244 | 257 | 1836")
                         & (df_local["ID_CATEGORIA"] == "5922 | 0 | 0 | 0 | 0")
                         ,"VALOR"].empty # Óbitos com idade menos de 1 ano
                        and 
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "2609")
                         & (df_local["ID_VARIAVEL"] == "217")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Nascidos Vivos
                    ) 
                    else None
        )
        
        
        
        # Quantidade de hospitais - Numerador
       ,"VAR_17_1": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "10051")
                   & (df_local["ID_VARIAVEL"] == "13324")
                   & (df_local["ID_CLASSIFICACAO"] == "3 | 14")
                   & (df_local["ID_CATEGORIA"] == "491 | 200")
                   ,"VALOR"], errors="coerce").iloc[0]
                   
               if not  df_local.loc[
                     (df_local["ID_AGREGADO"] == "10051")
                     & (df_local["ID_VARIAVEL"] == "13324")
                     & (df_local["ID_CLASSIFICACAO"] == "3 | 14")
                     & (df_local["ID_CATEGORIA"] == "491 | 200")
                     ,"VALOR"].empty else None
               
        # Total população - Denominador
        ,"VAR_17_2": pd.to_numeric(df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"], errors="coerce").iloc[0]
            if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "4714")
                    & (df_local["ID_VARIAVEL"] == "93")
                    # ID_CLASSIFICACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None
                
        # Quantidade de hospitais por 1.000 habitantes
        ,"VAR_17": ((
                    pd.to_numeric(df_local.loc[
                                (df_local["ID_AGREGADO"] == "10051")
                                & (df_local["ID_VARIAVEL"] == "13324")
                                & (df_local["ID_CLASSIFICACAO"] == "3 | 14")
                                & (df_local["ID_CATEGORIA"] == "491 | 200")
                                ,"VALOR"], errors="coerce").iloc[0] # Quantidade de hospitais - Numerador
                    /        
                    pd.to_numeric(df_local.loc[
                                (df_local["ID_AGREGADO"] == "4714")
                                & (df_local["ID_VARIAVEL"] == "93")
                                # ID_CLASSIFICACAO N/A
                                # ID_CATEGORIA N/A
                                ,"VALOR"], errors="coerce").iloc[0]  # Total população
                    ) 
                    *1000 # por 1.000 nascidos vivos 
                    if (
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "10051")
                         & (df_local["ID_VARIAVEL"] == "13324")
                         & (df_local["ID_CLASSIFICACAO"] == "3 | 14")
                         & (df_local["ID_CATEGORIA"] == "491 | 200")
                         ,"VALOR"].empty # Quantidade de hospitais - Numerador
                        and 
                        not df_local.loc[
                         (df_local["ID_AGREGADO"] == "4714")
                         & (df_local["ID_VARIAVEL"] == "93")
                         # ID_CLASSIFICACAO N/A
                         # ID_CATEGORIA N/A
                         ,"VALOR"].empty # Total população
                    ) 
                    else None
        )
        
        
        
        
        # Salário médio mensal dos trabalhadores formais (salários mínimos)
       ,"VAR_18": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "1606")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR" ], errors="coerce").iloc[0]
               if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "1606")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR"].empty else None
               
        # Salário médio mensal dos trabalhadores formais (reais)
       ,"VAR_19": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "10143")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR" ], errors="coerce").iloc[0]
               if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "10143")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR"].empty else None
           
       # Pessoal ocupado em postos de trabalho formais
       ,"VAR_20": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "707")
                   # ID_CLASSIFIACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR"], errors="coerce").iloc[0]
               if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "707")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR"].empty else None
        
       # Taxa de Ocupação - Numerador - Pessoal ocupado em postos de trabalho formais
       ,"VAR_21_1": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "9509")
                   & (df_local["ID_VARIAVEL"] == "707")
                   # ID_CLASSIFICACAO N/A
                   # ID_CATEGORIA N/A
                   ,"VALOR" ], errors="coerce").iloc[0]
         
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9509")
                    & (df_local["ID_VARIAVEL"] == "707")
                    # ID_CLASSIFIACAO N/A
                    # ID_CATEGORIA N/A
                    ,"VALOR"].empty else None

        # Taxa de Ocupação - Denominador - Soma da população de 18 a 65 anos
        ,"VAR_21_2": (df_local.loc[
                     (df_local["ID_AGREGADO"] == "9606")
                     & (df_local["ID_VARIAVEL"] == "93")
                     & (df_local["ID_CLASSIFICACAO"] == "287 | 2 | 86")
                     & (df_local["ID_CATEGORIA"].isin([
                         "6575 | 6794 | 95251",  # 18 anos
                         "6576 | 6794 | 95251",  # 19 anos
                         "93087 | 6794 | 95251", # 20 a 24 anos
                         "93088 | 6794 | 95251", # 25 a 29 anos
                         "93089 | 6794 | 95251", # 30 a 34 anos
                         "93090 | 6794 | 95251", # 35 a 39 anos
                         "93091 | 6794 | 95251", # 40 a 44 anos
                         "93092 | 6794 | 95251", # 45 a 49 anos
                         "93093 | 6794 | 95251" ,# 50 a 54 anos
                         "93094 | 6794 | 95251", # 55 a 59 anos
                         "93095 | 6794 | 95251", # 60 a 64 anos
                         "6618 | 6794 | 95251",  # 65 anos
                     ]))
                     , ["ID_CATEGORIA", "VALOR"]].drop_duplicates() # considera só colunas relevantes para deduplicar
                     ["VALOR"].apply(pd.to_numeric, errors="coerce")).sum()  # Soma da população de 18 a 65 anos
                if not df_local.loc[
                    (df_local["ID_AGREGADO"] == "9606")
                    & (df_local["ID_VARIAVEL"] == "93")
                    & (df_local["ID_CLASSIFICACAO"] == "287 | 2 | 86")
                    & (df_local["ID_CATEGORIA"].isin([
                        "6575 | 6794 | 95251",  # 18 anos
                        "6576 | 6794 | 95251",  # 19 anos
                        "93087 | 6794 | 95251", # 20 a 24 anos
                        "93088 | 6794 | 95251", # 25 a 29 anos
                        "93089 | 6794 | 95251", # 30 a 34 anos
                        "93090 | 6794 | 95251", # 35 a 39 anos
                        "93091 | 6794 | 95251", # 40 a 44 anos
                        "93092 | 6794 | 95251", # 45 a 49 anos
                        "93093 | 6794 | 95251" ,# 50 a 54 anos
                        "93094 | 6794 | 95251", # 55 a 59 anos
                        "93095 | 6794 | 95251", # 60 a 64 anos
                        "6618 | 6794 | 95251",  # 65 anos
                    ]))
                    , ["ID_CATEGORIA", "VALOR"]].drop_duplicates() # considera só colunas relevantes para deduplicar
                    ["VALOR"].empty else None

       # Taxa de Ocupação (Pessoal ocupado em postos de trabalho formais / População de 18 a 65 anos)
       ,"VAR_21": ((
                   pd.to_numeric(df_local.loc[
                               (df_local["ID_AGREGADO"] == "9509")
                               & (df_local["ID_VARIAVEL"] == "707")
                               # ID_CLASSIFICACAO N/A
                               # ID_CATEGORIA N/A
                               ,"VALOR" ], errors="coerce").iloc[0] # Pessoal ocupado em postos de trabalho formais
                   /        
                   (df_local.loc[
                                (df_local["ID_AGREGADO"] == "9606")
                                & (df_local["ID_VARIAVEL"] == "93")
                                & (df_local["ID_CLASSIFICACAO"] == "287 | 2 | 86")
                                & (df_local["ID_CATEGORIA"].isin([
                                    "6575 | 6794 | 95251",  # 18 anos
                                    "6576 | 6794 | 95251",  # 19 anos
                                    "93087 | 6794 | 95251", # 20 a 24 anos
                                    "93088 | 6794 | 95251", # 25 a 29 anos
                                    "93089 | 6794 | 95251", # 30 a 34 anos
                                    "93090 | 6794 | 95251", # 35 a 39 anos
                                    "93091 | 6794 | 95251", # 40 a 44 anos
                                    "93092 | 6794 | 95251", # 45 a 49 anos
                                    "93093 | 6794 | 95251" ,# 50 a 54 anos
                                    "93094 | 6794 | 95251", # 55 a 59 anos
                                    "93095 | 6794 | 95251", # 60 a 64 anos
                                    "6618 | 6794 | 95251",  # 65 anos
                                ]))
                                , ["ID_CATEGORIA", "VALOR"]].drop_duplicates() # considera só colunas relevantes para deduplicar
                                ["VALOR"].apply(pd.to_numeric, errors="coerce")).sum() # Soma da população de 18 a 65 anos
                        )*100 
                   if (
                      not df_local.loc[
                          (df_local["ID_AGREGADO"] == "9509")
                          & (df_local["ID_VARIAVEL"] == "707")
                          # ID_CLASSIFIACAO N/A
                          # ID_CATEGORIA N/A
                          ,"VALOR"].empty # Pessoal ocupado em postos de trabalho formais 
                      and
                      not df_local.loc[
                          (df_local["ID_AGREGADO"] == "9606")
                          & (df_local["ID_VARIAVEL"] == "93")
                          & (df_local["ID_CLASSIFICACAO"] == "287 | 2 | 86")
                          & (df_local["ID_CATEGORIA"].isin([
                              "6575 | 6794 | 95251",  # 18 anos
                              "6576 | 6794 | 95251",  # 19 anos
                              "93087 | 6794 | 95251", # 20 a 24 anos
                              "93088 | 6794 | 95251", # 25 a 29 anos
                              "93089 | 6794 | 95251", # 30 a 34 anos
                              "93090 | 6794 | 95251", # 35 a 39 anos
                              "93091 | 6794 | 95251", # 40 a 44 anos
                              "93092 | 6794 | 95251", # 45 a 49 anos
                              "93093 | 6794 | 95251" ,# 50 a 54 anos
                              "93094 | 6794 | 95251", # 55 a 59 anos
                              "93095 | 6794 | 95251", # 60 a 64 anos
                              "6618 | 6794 | 95251",  # 65 anos
                          ]))
                          , ["ID_CATEGORIA", "VALOR"]].drop_duplicates() # considera só colunas relevantes para deduplicar
                          ["VALOR"].empty # Soma da população de 18 a 65 anos
                     ) 
                   else None
                   )
       
       # Taxa de esgotamento sanitário por rede geral, rede pluvial ou fossa ligada à rede
       ,"VAR_22": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "6805")
                   & (df_local["ID_VARIAVEL"] == "1000381")
                   & (df_local["ID_CLASSIFICACAO"] == "11558")
                   & (df_local["ID_CATEGORIA"] == "46290")
                   ,"VALOR"], errors="coerce").iloc[0]
           if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "6805")
                   & (df_local["ID_VARIAVEL"] == "1000381")
                   & (df_local["ID_CLASSIFICACAO"] == "11558")
                   & (df_local["ID_CATEGORIA"] == "46290")
                   ,"VALOR"].empty else None
       
       # Taxa de domicílios com coleta de lixo
       ,"VAR_23": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "6892")
                   & (df_local["ID_VARIAVEL"] == "1000381")
                   & (df_local["ID_CLASSIFICACAO"] == "67")
                   & (df_local["ID_CATEGORIA"] == "2520")
                   ,"VALOR"], errors="coerce").iloc[0]
           if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "6892")
                   & (df_local["ID_VARIAVEL"] == "1000381")
                   & (df_local["ID_CLASSIFICACAO"] == "67")
                   & (df_local["ID_CATEGORIA"] == "2520")
                   ,"VALOR"].empty else None
       
       # Taxa de domicílios com internet
       ,"VAR_24": pd.to_numeric(df_local.loc[
                   (df_local["ID_AGREGADO"] == "9936")
                   & (df_local["ID_VARIAVEL"] == "1000381")
                   & (df_local["ID_CLASSIFICACAO"] == "2072 | 63 | 125")
                   & (df_local["ID_CATEGORIA"] == "77585 | 95826 | 2932")
                   ,"VALOR"], errors="coerce").iloc[0]
           if not df_local.loc[
                   (df_local["ID_AGREGADO"] == "9936")
                   & (df_local["ID_VARIAVEL"] == "1000381")
                   & (df_local["ID_CLASSIFICACAO"] == "2072 | 63 | 125")
                   & (df_local["ID_CATEGORIA"] == "77585 | 95826 | 2932")
                   ,"VALOR"].empty else None
    })


# Gera o DataFrame final
df_variaveis_municipios = pd.DataFrame(ls_variaveis_municipios)


### GERAR RANK DAS VARIAVEIS

# Obtem as colunas das variaveis
colunas_variaveis = df_variaveis_municipios.filter(regex=r'^VAR_').columns.tolist()

for var in colunas_variaveis:
    
    # Obter o criterio de ranking
    criterio_rank = mapa_criterio_rank.get(var)
    nova_coluna = "RANK_"+var
    
    
    if criterio_rank == "descrescente":
       df_variaveis_municipios[nova_coluna] = df_variaveis_municipios[var].rank(ascending=False, method='dense')
    
    if criterio_rank == "crescente":
        df_variaveis_municipios[nova_coluna] = df_variaveis_municipios[var].rank(ascending=True, method='dense')
    
    fnc_tcc.log(f"Criado a coluna {nova_coluna} com Ranking {criterio_rank}")


# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_dataframe_csv (df_variaveis_municipios,diretorio_arquivos+"/rotina","df_variaveis_municipios.csv")  

# Salva analise dos indicadores
df_estatisticas_variaveis_municipios = pd.DataFrame(df_variaveis_municipios.describe())



#%% ANALISE DESCRITIVA DAS VARIAVEIS

# Se não localizar o df_variaveis_municipios, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_municipios', globals()):
    fnc_tcc.log("DataFrame df_variaveis_municipios não existente, iniciando importação do backup")
    chek_arq, df_variaveis_municipios = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_municipios.csv",True,True)



# Gerando a analise descritiva
df_analise_descritiva = pd.DataFrame({
        'QTD_PREENCHIDO': df_variaveis_municipios.notna().sum(),
        'QTD_VAZIO': df_variaveis_municipios.isna().sum(),
        'MINIMO': df_variaveis_municipios.min(numeric_only=True),
        'MEDIA': df_variaveis_municipios.mean(numeric_only=True),
        'DESVIO_PADRAO': df_variaveis_municipios.std(numeric_only=True) ,
        'MAXIMO': df_variaveis_municipios.max(numeric_only=True)
        })


# Incluindo a descrição da varíavel
df_analise_descritiva["DESCRICAO_VARIAVEL"] = (
    df_analise_descritiva.index.map(mapa_descricao_var)
)


# Incluindo o grupo da varíavel
df_analise_descritiva["GRUPO_INDICADOR"] = (
    df_analise_descritiva.index.map(mapa_grupo_var)
)


# Incluindo a unidade de medida da varíavel
df_analise_descritiva["UNIDADE_MEDIDA"] = (
    df_analise_descritiva.index.map(mapa_unidade_var)
)

# Organizando as colunas
colunas = ['GRUPO_INDICADOR']+['DESCRICAO_VARIAVEL']+ ['UNIDADE_MEDIDA'] + [col for col in df_analise_descritiva.columns if col != 'DESCRICAO_VARIAVEL' and col != 'UNIDADE_MEDIDA' and col != 'GRUPO_INDICADOR']
df_analise_descritiva = df_analise_descritiva[colunas]
df_analise_descritiva

# Arredondando as colunas
for idx, row in df_analise_descritiva.iterrows():
    
    casas = mapa_casas_decimais.get(row['UNIDADE_MEDIDA'], 2)

    df_analise_descritiva.at[idx, 'MINIMO'] = round(row['MINIMO'], casas)
    df_analise_descritiva.at[idx, 'MEDIA'] = round(row['MEDIA'], casas+1)
    df_analise_descritiva.at[idx, 'DESVIO_PADRAO'] = round(row['DESVIO_PADRAO'], casas+1)
    df_analise_descritiva.at[idx, 'MAXIMO'] = round(row['MAXIMO'], casas)
   
    
# Excluindo as linhas sem descrição
df_analise_descritiva = df_analise_descritiva.dropna(subset=['DESCRICAO_VARIAVEL'])

fnc_tcc.log("Gerado analise descritiva das variaveis utilizadas")
fnc_tcc.exportar_dataframe_csv (df_analise_descritiva,diretorio_arquivos+"/rotina","df_analise_descritiva.csv")  

# Gerar Boxplot das variaveis
fnc_tcc.gerar_boxplot(
                        df_variaveis_municipios
                         ,colunas_filtrar=ls_variaveis
                         ,rotacao_x=90
                         ,titulo='Boxplot de todas as variáveis'
                         ,arquivo_saida=diretorio_imagens+"/bloxpot_variaveis.png"
                         ,figsize=(9, 4)
                         )

# Gera Histrograma de preenchimento das variaveis
qtde_preenchidos = df_variaveis_municipios[ls_variaveis].notna().sum()

fnc_tcc.gerar_histograma(qtde_preenchidos
                ,titulo='Distribuição da quantidade de registros preenchidos por variável'
                ,titulo_x='Variáveis'
                ,titulo_y='Quantidade de registros preenchidos'
                ,arquivo_saida = diretorio_imagens+"/histograma_variaveis.png"
                )



#%% CONSULTA CIDADE

id_municipio = 3530607

# Se não localizar o df_variaveis_municipios, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_municipios', globals()):
    fnc_tcc.log("DataFrame df_variaveis_municipios não existente, iniciando importação do backup")
    chek_arq, df_variaveis_municipios = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_municipios.csv",True,True)


# Obtem os registros da cidade
df_variaveis_cidade_selecionada = df_variaveis_municipios[df_variaveis_municipios["ID_LOCALIDADE"] == id_municipio]


# Exibe os detalhes cadastrais da cidade
# Loop para exibir os indicadores
for indicador_cidade in df_variaveis_cidade_selecionada.itertuples(index=False):
    print(f'Município: {indicador_cidade.ID_LOCALIDADE} - {indicador_cidade.NOME_LOCALIDADE}')

# Loop para exibir os indicadores
for variavel in df_biblioteca.itertuples(index=False):
    
   
    # Trata/refina formatação do valor
    casas_decimal = mapa_casas_decimais.get(variavel.UNIDADE_MEDIDA, 2)
    valor_arredondado = round(df_variaveis_cidade_selecionada[variavel.REFERENCIA_INDICADOR].iloc[0], casas_decimal)
    valor_formatado = f"{valor_arredondado:,.{casas_decimal}f}".replace(',', 'X').replace('.', ',').replace('X', '.') 
    
    print(f"{variavel.DESCRICAO_VARIAVEL} ({variavel.PERIODO}): {valor_formatado} {variavel.UNIDADE_MEDIDA}")



#%% NORMALIZA VARIAVEIS

# Configura para que o pandad exiba todas as colunas e linhas completas
pd.set_option('display.max_columns', None)  # Mostra todas as colunas
pd.set_option('display.max_rows', None)     # Mostra todas as linhas
pd.set_option('display.width', None)        # Ajusta a largura total
pd.set_option('display.max_colwidth', None) # Mostra colunas completas (sem truncar texto)

# Se não localizar o df_variaveis_municipios, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_municipios', globals()):
    fnc_tcc.log("DataFrame df_variaveis_municipios não existente, iniciando importação do backup")
    chek_arq, df_variaveis_municipios = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_municipios.csv",True,True)


# Retira Colunas que identifica localidade
colunas_deconsiderar = ['ID_LOCALIDADE', 'NOME_LOCALIDADE']
df_variaveis = df_variaveis_municipios.drop(columns=colunas_deconsiderar).copy()

# Retira Colunas de Ranking
colunas_deconsiderar = df_variaveis.filter(regex=r'^RANK_').columns.tolist()
df_variaveis = df_variaveis.drop(columns=colunas_deconsiderar).copy()

# Retira Colunas Auxiliares (contem 2 _)
colunas_deconsiderar = df_variaveis.filter(regex=r'.*_.+_.*').columns.tolist()
df_variaveis = df_variaveis.drop(columns=colunas_deconsiderar).copy()

print('Análise Descritiva antes da normalização')
print(df_variaveis.describe().T[['count', 'mean', 'std', 'min', 'max']].round(2))
print('\nValores ausentes por coluna:')
print(df_variaveis.isna().sum())

# Pipeline: (1) imputação da medianda (2) normalização 
pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),  # trata valores NaN
    ('scaler', StandardScaler())                    # Z-Score: média = 0, desvio = 1
])


# Executa pipeline e normaliza os dados
variaveis_normalizadas = pipeline.fit_transform(df_variaveis)

# Gera DataFrame com as variaveis normalizadas
df_variaveis_normalizadas = pd.DataFrame(variaveis_normalizadas, columns=df_variaveis.columns, index=df_variaveis.index)
fnc_tcc.exportar_dataframe_csv (df_variaveis_normalizadas,diretorio_arquivos+"/rotina","df_variaveis_normalizadas.csv")  

print('\n Análise Descritiva após normalização')
print(df_variaveis_normalizadas.describe().T[['count', 'mean', 'std', 'min', 'max']].round(2))
print('\nValores ausentes por coluna após normalização:')
print(df_variaveis_normalizadas.isna().sum())

# Reseta as configuracoes do pandas
pd.reset_option('all')

# Gera DataFrame Consolidado que irá consolidar todos os detalhes
df_consolidado_variaveis = pd.concat([df_variaveis_municipios, df_variaveis_normalizadas.add_suffix('_NORM')], axis=1)
fnc_tcc.exportar_dataframe_csv (df_consolidado_variaveis,diretorio_arquivos+"/rotina","df_consolidado_variaveis.csv")  

# Exporta as analises descrivas
fnc_tcc.exportar_dataframe_csv (pd.DataFrame(df_variaveis_municipios.describe().T),diretorio_arquivos+"/rotina","describe_df_variaveis_municipios.csv",True)  
fnc_tcc.exportar_dataframe_csv (pd.DataFrame(df_variaveis_normalizadas.describe().T),diretorio_arquivos+"/rotina","describe_df_variaveis_normalizadas.csv",True)  
fnc_tcc.exportar_dataframe_csv (pd.DataFrame(df_consolidado_variaveis.describe().T),diretorio_arquivos+"/rotina","describe_df_consolidado_variaveis.csv",True)  


# Gerar Boxplot das variaveis normalizadas
fnc_tcc.gerar_boxplot(
                        df_variaveis_normalizadas
                         #,colunas_filtrar=
                         ,rotacao_x=90
                         ,titulo='Boxplot de todas as variáveis normalizadas'
                         ,arquivo_saida=diretorio_imagens+"/bloxpot_variaveis_normalizadas.png"
                         ,figsize=(9, 4)
                         )

# Gera Histrograma de preenchimento das variaveis normalizadas
qtde_preenchidos = df_variaveis_normalizadas.notna().sum()

fnc_tcc.gerar_histograma(qtde_preenchidos
                ,titulo='Distribuição da quantidade de registros preenchidos por variável'
                ,titulo_x='Variáveis Normalizadas'
                ,titulo_y='Quantidade de registros preenchidos'
                ,arquivo_saida = diretorio_imagens+"/histograma_variaveis_normalizadas.png"
                )

#%%  ANALISE DE CORRELACAO DE PEARSON DE TODAS VARIAVEIS - VARIAVEIS NORMALIZADAS

# Se não localizar o df_variaveis_normalizadas, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_normalizadas', globals()):
    fnc_tcc.log("DataFrame df_variaveis_normalizadas não existente, iniciando importação do backup")
    chek_arq, df_variaveis_normalizadas = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_normalizadas.csv",True,True)

# Exibe Correlacao de Pearson
print(pg.rcorr(df_variaveis_normalizadas
         ,method = 'pearson'
         ,upper='pval'
         ,decimals = 3
         ,pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}
         ))


# Caminho que a imagem será disponibilizada
caminho_corr = diretorio_imagens+"/"+"todas_var_matriz_correlacao.png"

# Gerando Correlação para Gerar o gráfico
corr_geral = df_variaveis_normalizadas.corr()

# Plotando em um gráfico heatmap    
plt.figure(figsize=(20, 18), dpi=600)
sns.heatmap(corr_geral, 
            cmap=plt.cm.coolwarm,
            vmax=1, 
            vmin=-1,
            center=0,
            square=True, 
            linewidths=.5,
            annot=True,
            fmt='.3f', 
            annot_kws={'size':13},
            cbar_kws={
                    "shrink": 0.75,
                    "pad": 0.01,
                    "aspect": 40
                    },
            
            )
# Fonte dos eixos

plt.title('Matriz de Correlações - Todas as Variáveis', fontsize=14)
plt.tight_layout()
plt.tick_params(labelsize=12)
plt.savefig(caminho_corr, dpi=300, bbox_inches="tight")
plt.show()
plt.close()
fnc_tcc.log(f"Matriz de Correlações 'Todas as Variáveis' gerado: {caminho_corr}")


#%%  ANALISE DE CORRELACAO DE PEARSON POR GRUPO DE INDICADORES - VARIAVEIS NORMALIZADAS

# Se não localizar o df_consolidado_variaveis, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_consolidado_variaveis', globals()):
    fnc_tcc.log("DataFrame df_consolidado_variaveis não existente, iniciando importação do backup")
    chek_arq, df_consolidado_variaveis = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_consolidado_variaveis.csv",True,True)


# Se não localizar o df_variaveis_normalizadas, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_normalizadas', globals()):
    fnc_tcc.log("DataFrame df_variaveis_normalizadas não existente, iniciando importação do backup")
    chek_arq, df_variaveis_normalizadas = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_normalizadas.csv",True,True)


# Obtem os grupos de indicadores
grupos_validos = (
    df_biblioteca['GRUPO_INDICADOR']
    .dropna()
    .unique()
)

# Data Frame que irá guardar as altas correlacoes para analise de indicador redundante
df_alta_correlacao_cons = pd.DataFrame(columns=[
                                                "PAR_ORDENADO"
                                                ,"VAR_1"
                                                ,"VAR_2"
                                                ,"CORRELACAO"
    ])

# Loop entre os grupos de indicadores
for nome_grupo_atual in grupos_validos:
    
    # Ignora grupos que contenham 'Auxiliar'
    if 'auxiliar' in nome_grupo_atual.lower() or 'rank' in nome_grupo_atual.lower():
        continue
    
    # Grupo Atual
    print("\n")
    fnc_tcc.log(f"GRUPO ATUAL '{nome_grupo_atual}'")
    
    # Obtem o padrao de abreviacao do grupo
    abrev_atual = mapa_abreviacoes_grupos.get(nome_grupo_atual,fnc_tcc.tratar_nome_arquivo(nome_grupo_atual))
    
    # Nome do Grupo tratado para utilizar nos DataFrames
    nome_grupo_trat = fnc_tcc.tratar_nome_arquivo(nome_grupo_atual)
    
    # Seleciona os indicadores normalizados do grupo atual
    fnc_tcc.log(f"Obtendo os indicadores normalizados de '{nome_grupo_atual}'")
    df_indicadores_grupo_atual = df_biblioteca[df_biblioteca['GRUPO_INDICADOR'] == nome_grupo_atual]
    
    # Obtem as colunas/indicadores Normalizados
    colunas = df_indicadores_grupo_atual['REFERENCIA_INDICADOR'].tolist()
    colunas_norm = [col + "_NORM" for col in colunas]
    
    # Cria DataFrame para analise de correlacao
    df_tempo_corr = df_consolidado_variaveis[colunas_norm].copy()
    
    # Exibe Correlacao de Pearson
    print(pg.rcorr(df_tempo_corr
             ,method = 'pearson'
             ,upper='pval'
             ,decimals = 3
             ,pval_stars = {0.01: '***', 0.05: '**', 0.10: '*'}
             ))
    
    
    # Caminho que a imagem será disponibilizada
    caminho_corr = diretorio_imagens+"/"+abrev_atual+"_matriz_correlacao.png"
    
    # Gerando Correlação para Gerar o gráfico
    corr = df_tempo_corr.corr()

    # Plotando em um gráfico heatmap    
    plt.figure(figsize=(12,8), dpi=600)
    sns.heatmap(corr, 
                cmap=plt.cm.coolwarm,
                vmax=1, 
                vmin=-1,
                center=0,
                square=True, 
                linewidths=.5,
                annot=True,
                fmt='.3f', 
                annot_kws={'size':12},
                cbar_kws={"shrink":0.50})
    
    plt.title(f'Matriz de Correlações - {nome_grupo_atual}', fontsize=14)
    plt.tight_layout()
    plt.tick_params(labelsize=10)
    plt.savefig(caminho_corr, dpi=300, bbox_inches="tight")
    plt.show()
    plt.close()
    fnc_tcc.log(f"Matriz de Correlações '{nome_grupo_atual}' gerado: {caminho_corr}")
    
    # Lista os partes de correlacao
    df_corr_pares = (
                    corr
                    .stack()
                    .reset_index()
                    .rename(columns={
                        "level_0": "VAR_1",
                        "level_1": "VAR_2",
                        0: "CORRELACAO"
                    })
                )
    
    # Filtra os casos com alta correlacao >=0.99 ou <=-0.99
    df_alta_correlacao = df_corr_pares[
        (df_corr_pares["VAR_1"] != df_corr_pares["VAR_2"]) &
        (
            (df_corr_pares["CORRELACAO"].abs() >= 0.99) |
            (df_corr_pares["CORRELACAO"].abs() <= -0.99)
        )
        ]
    
    # Deduplica
    df_alta_correlacao["PAR_ORDENADO"] = df_alta_correlacao.apply(
    lambda x: tuple(sorted([x["VAR_1"], x["VAR_2"]])),
    axis=1
    )
    
    # Deduplica as combinacoes
    df_alta_correlacao = df_alta_correlacao.drop_duplicates(subset="PAR_ORDENADO")
    
    # Empilha com altas correlacoes de outros grupos
    df_alta_correlacao_cons = pd.concat([df_alta_correlacao_cons,df_alta_correlacao], axis=0)
    

# Se identificado altas correlações sinaliza
if len(df_alta_correlacao_cons)>0:
    
    print("\n")
    fnc_tcc.log(f"⚠️ Identificado {len(df_alta_correlacao_cons)} casos de alta correlação, analisar possíveis indicadores redundantes")
    
    print(df_alta_correlacao_cons)
    
    # Exporta as df_alta_correlacao_cons descrivas
    fnc_tcc.exportar_dataframe_csv (df_alta_correlacao_cons,diretorio_arquivos+"/rotina","df_alta_correlacao_cons.csv",True)  


#%% GERAR CLUSTERS POR GRUPO DE INDICADORES

# Se não localizar o df_biblioteca, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_biblioteca', globals()):
    fnc_tcc.log("DataFrame df_biblioteca não existente, iniciando importação do backup")
    chek_arq, df_biblioteca = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_biblioteca.csv",True,True)


# Se não localizar o df_consolidado_variaveis, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_consolidado_variaveis', globals()):
    fnc_tcc.log("DataFrame df_consolidado_variaveis não existente, iniciando importação do backup")
    chek_arq, df_consolidado_variaveis = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_consolidado_variaveis.csv",True,True)

# Se não localizar o df_variaveis_normalizadas, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_normalizadas', globals()):
    fnc_tcc.log("DataFrame df_variaveis_normalizadas não existente, iniciando importação do backup")
    chek_arq, df_variaveis_normalizadas = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_normalizadas.csv",True,True)


## Parametros

# Retira Variaveis redundantes, com alto grau de correlacao (1 ou -1) com outra variaval
colunas_redundantes = ['VAR_05','VAR_09','VAR_19']

# Dicionario para organizar os DataFrame dos grupos
dict_indicadores_por_grupo = {}

# Dicionario para organizar indicadores das execucoes por grupos
dict_clusters_por_grupo = {}

# Dicionario para organizar metricas por grupos
dict_metricas_por_grupo = {} 

# Ks a testar
k_values = range(100, 501, 5)  # range de 100 a 500 incrementando 5
#k_values = range(100, 110, 10)  

# Copia DataFrame com o consolidado das variaveis retirando as colunas redundantes
df_consolidado_variaveis_clusters = df_consolidado_variaveis.copy()

# Obtem os grupos de indicadores
grupos_validos = (
    df_biblioteca['GRUPO_INDICADOR']
    .dropna()
    .unique()
)


# Loop entre os grupos de indicadores
for nome_grupo_atual in grupos_validos:
    
    # Ignora grupos que contenham 'Auxiliar'
    if 'auxiliar' in nome_grupo_atual.lower() or 'rank' in nome_grupo_atual.lower():
        continue
    
    # Grupo Atual
    print("\n")
    fnc_tcc.log(f"GRUPO ATUAL '{nome_grupo_atual}'")
    
    # Obtem o padrao de abreviacao do grupo
    abrev_atual = mapa_abreviacoes_grupos.get(nome_grupo_atual,fnc_tcc.tratar_nome_arquivo(nome_grupo_atual))
    
    # Nome do Grupo tratado para utilizar nos DataFrames
    nome_grupo_trat = fnc_tcc.tratar_nome_arquivo(nome_grupo_atual)
    
    # Seleciona os indicadores normalizados do grupo atual
    fnc_tcc.log(f"Obtendo os indicadores normalizados de '{nome_grupo_atual}'")
    df_indicadores_grupo_atual = df_biblioteca[df_biblioteca['GRUPO_INDICADOR'] == nome_grupo_atual]
    
    # Obtem as colunas/indicadores - retirando as que deverão ser desconsideradas
    colunas = df_indicadores_grupo_atual['REFERENCIA_INDICADOR'].tolist()
    colunas_norm = [col + "_NORM" 
                    for col in colunas 
                    if (col in df_variaveis_normalizadas.columns
                        and col not in colunas_redundantes
                        )
                    ]
    
    # Cria DataFrame  com as variaveis normalizadas do grupo atual
    nome_df_var = 'df_indicadores_'+abrev_atual
    dict_indicadores_por_grupo[nome_df_var] = df_consolidado_variaveis[colunas_norm].copy()
    fnc_tcc.log(f"Criado o '{nome_df_var}' criado com {len(colunas_norm)} colunas")
       
    # Executar Clusterizacao
    fnc_tcc.log(f"Executando Clusterização do '{nome_grupo_atual}'")
    df_clusters_grupo_atual, df_metricas_grupo_atual = fnc_tcc.gerar_clusters_kmeans(dict_indicadores_por_grupo[nome_df_var], k_values)
    
    # Cria DataFrame das clusters do grupo atual
    nome_df_clusters = 'df_clusters_'+abrev_atual
    dict_clusters_por_grupo[nome_df_clusters] = df_clusters_grupo_atual.copy()
    
    # Cria DataFrame das metricas do grupo atual
    nome_df_metricas = 'df_metricas_'+abrev_atual
    nome_df_metricas_norm = 'df_metricas_norm_'+abrev_atual
    dict_metricas_por_grupo[nome_df_metricas] = df_metricas_grupo_atual.copy()
    
    
    # Gerar Boxplot das variaveis - Não normalizado
    fnc_tcc.gerar_boxplot(
                            df_consolidado_variaveis[colunas]
                             #,colunas_filtrar=colunas
                             ,rotacao_x=90
                             ,titulo='Boxplot das varíaveis do Grupo Indicador '+nome_grupo_atual
                             ,arquivo_saida= diretorio_imagens +'/'+ abrev_atual +"_bloxpot.png"
                             ,figsize=(9, 4)
                             )
    
    # Gera Histrograma de preenchimento das variaveis - Não normalizado
    qtde_preenchidos = df_consolidado_variaveis[colunas].notna().sum()
    
    fnc_tcc.gerar_histograma(qtde_preenchidos
                    ,titulo='Distribuição da quantidade de registros preenchidos por variável do Grupo Indicador '+nome_grupo_atual
                    ,titulo_x='Variáveis'
                    ,titulo_y='Quantidade de registros preenchidos'
                    ,arquivo_saida = diretorio_imagens+'/'+ abrev_atual +"_histograma.png"
                    )

    
    # Gerar Boxplot das variaveis - Normalizado
    fnc_tcc.gerar_boxplot(
                            df_consolidado_variaveis[colunas_norm]
                             #,colunas_filtrar=colunas
                             ,rotacao_x=90
                             ,titulo='Boxplot das varíaveis do Grupo Indicador '+nome_grupo_atual
                             ,arquivo_saida= diretorio_imagens +'/'+ abrev_atual +"_bloxpot_normalizado.png"
                             ,figsize=(9, 4)
                             )
    
    # Gera Histrograma de preenchimento das variaveis - Não normalizado
    qtde_preenchidos = df_consolidado_variaveis[colunas_norm].notna().sum()
    
    fnc_tcc.gerar_histograma(qtde_preenchidos
                    ,titulo='Distribuição da quantidade de registros preenchidos por variável normalizada do Grupo Indicador '+nome_grupo_atual
                    ,titulo_x='Variáveis Normalizadas'
                    ,titulo_y='Quantidade de registros preenchidos'
                    ,arquivo_saida = diretorio_imagens+'/'+ abrev_atual +"_histograma_normalizado.png"
                    )

# Configura para que o pandad exiba todas as colunas e linhas completas
pd.set_option('display.max_columns', None)  # Mostra todas as colunas
pd.set_option('display.max_rows', None)     # Mostra todas as linhas
pd.set_option('display.width', None)        # Ajusta a largura total
pd.set_option('display.max_colwidth', None) # Mostra colunas completas (sem truncar texto)

# Exibe analise descritiva para simples consulta
print(df_consolidado_variaveis_clusters.describe().T[['count', 'mean', 'std', 'min', 'max']].round(2))

# Reseta as configuracoes do pandas
pd.reset_option('all')

# Exporta dados para efeito de backup e evitar reprocessamento futuro
fnc_tcc.exportar_pickle(dict_indicadores_por_grupo,diretorio_arquivos+"/rotina","dict_indicadores_por_grupo.pkl")
fnc_tcc.exportar_pickle(dict_clusters_por_grupo,diretorio_arquivos+"/rotina","dict_clusters_por_grupo.pkl")
fnc_tcc.exportar_pickle(dict_metricas_por_grupo,diretorio_arquivos+"/rotina","dict_metricas_por_grupo.pkl")


#%% DETALHA METRICAS DAS CLUSTERIZACOES E OBTEM O MELHOR K POR GRUPO INDICADOR

# Se não localizar o dict_indicadores_por_grupo, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('dict_indicadores_por_grupo', globals()):
    fnc_tcc.log("dict_indicadores_por_grupo não existente, iniciando importação do backup")
    dict_indicadores_por_grupo = fnc_tcc.importar_pickle(diretorio_arquivos+"/rotina","dict_indicadores_por_grupo.pkl")

# Se não localizar o dict_clusters_por_grupo, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('dict_clusters_por_grupo', globals()):
    fnc_tcc.log("dict_clusters_por_grupo não existente, iniciando importação do backup")
    dict_clusters_por_grupo = fnc_tcc.importar_pickle(diretorio_arquivos+"/rotina","dict_clusters_por_grupo.pkl")

# Se não localizar o dict_metricas_por_grupo, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('dict_metricas_por_grupo', globals()):
    fnc_tcc.log("dict_metricas_por_grupo não existente, iniciando importação do backup")
    dict_metricas_por_grupo = fnc_tcc.importar_pickle(diretorio_arquivos+"/rotina","dict_metricas_por_grupo.pkl")

# Se não localizar o df_consolidado_variaveis, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_consolidado_variaveis', globals()):
    fnc_tcc.log("DataFrame df_consolidado_variaveis não existente, iniciando importação do backup")
    chek_arq, df_consolidado_variaveis = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_consolidado_variaveis.csv",True,True)

# Se não localizar o df_variaveis_normalizadas, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_variaveis_normalizadas', globals()):
    fnc_tcc.log("DataFrame df_variaveis_normalizadas não existente, iniciando importação do backup")
    chek_arq, df_variaveis_normalizadas = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_variaveis_normalizadas.csv",True,True)


## Parametros

# Dicionario para organizar metricas normalizadas por grupos
df_metricas_norm_por_grupo = {} 

# Dicionario para organizar medias das variaveis por grupos
df_medias_por_grupo = {} 

# Dicionario para organizar o melhor K por grupo
ls_melhor_k_por_grupo = []

# Copia DataFrame com o consolidado das variaveis retirando as colunas redundantes
df_consolidado_variaveis_clusters = df_consolidado_variaveis.copy()

# Obtem os grupos de indicadores
grupos_validos = (
    df_biblioteca['GRUPO_INDICADOR']
    .dropna()
    .unique()
)


# Loop entre os grupos de indicadores
for nome_grupo_atual in grupos_validos:
    
    # Ignora grupos que contenham 'Auxiliar'
    if 'auxiliar' in nome_grupo_atual.lower() or 'rank' in nome_grupo_atual.lower():
        continue
    
    # Grupo Atual
    print("\n")
    fnc_tcc.log(f"GRUPO ATUAL '{nome_grupo_atual}'")
    
    # Obtem o padrao de abreviacao do grupo
    abrev_atual = mapa_abreviacoes_grupos.get(nome_grupo_atual,fnc_tcc.tratar_nome_arquivo(nome_grupo_atual))
    
    # Nome do Grupo tratado para utilizar nos DataFrames
    nome_grupo_trat = fnc_tcc.tratar_nome_arquivo(nome_grupo_atual)
    
    # Seleciona os indicadores normalizados do grupo atual
    fnc_tcc.log(f"Obtendo os indicadores normalizados de '{nome_grupo_atual}'")
    df_indicadores_grupo_atual = df_biblioteca[df_biblioteca['GRUPO_INDICADOR'] == nome_grupo_atual]
    
    # Obtem as colunas/indicadores - retirando as que deverão ser desconsideradas
    colunas = df_indicadores_grupo_atual['REFERENCIA_INDICADOR'].tolist()
    
    
    # Variaveis do grupo atual
    nome_df_metricas = 'df_metricas_'+abrev_atual
    nome_df_metricas_norm = 'df_metricas_norm_'+abrev_atual
    nome_df_clusters = 'df_clusters_'+abrev_atual
    
    
    # Identifica o melhor k baseado na média das metricas normalizadas
    melhor_k_grupo, df_metricas_grupo_atual_norm = fnc_tcc.identificar_melhor_k(dict_metricas_por_grupo[nome_df_metricas])
    fnc_tcc.log(f"Identificado o k '{melhor_k_grupo}' como o melhor 'k' de '{nome_grupo_atual}'")
    
    
    # Obtem as metricas do grupo atual
    df_metricas_grupo_atual = dict_metricas_por_grupo[nome_df_metricas].copy()
    
    
    print('\n Análise Descritiva após normalização')
    print(df_metricas_grupo_atual_norm.describe().T[['count', 'mean', 'std', 'min', 'max']].round(2))

	# Cria DataFrame das metricas do grupo atual normalizado
    df_metricas_norm_por_grupo[nome_df_metricas_norm] = df_metricas_grupo_atual_norm.copy()
    
    # Recupera as métricasd do melhor k
    metricas_melhor_k = df_metricas_grupo_atual.loc[df_metricas_grupo_atual['k'] == melhor_k_grupo].iloc[0]
    
    # Empilha o melhor k
    ls_melhor_k_por_grupo.append({
                            'GRUPO_INDICADOR': nome_grupo_atual
                            ,'MELHOR_K': melhor_k_grupo
                            ,'Razao_Variancia': metricas_melhor_k.get('Razao_Variancia')
                            ,'BIC': metricas_melhor_k.get('BIC')
                            ,'Silhouette_Score': metricas_melhor_k.get('Silhouette_Score')
                            ,'Indice_Jaccard': metricas_melhor_k.get('Indice_Jaccard')
                        })
    

    
    # Gera coluna com clusters do melhor k no df_consolidado_variaveis
    nome_coluna = f"CLUSTER_{abrev_atual.upper()}"
    df_consolidado_variaveis_clusters[nome_coluna] = dict_clusters_por_grupo[nome_df_clusters][f'cluster_k{melhor_k_grupo}']
    fnc_tcc.log(f"Criado a coluna '{nome_coluna}' no df_consolidado_variaveis_clusters")
    
    # Gera analise de médias por cluster
    fnc_tcc.log(f"Gerando análise de médias por cluster para '{nome_grupo_atual}'")

    # Seleciona somente os indicadores do grupo + cluster
    df_para_media_grupo_atual = df_consolidado_variaveis_clusters[colunas + [nome_coluna]].copy()
    
    # Calcula médias por cluster
    df_medias_grupo_atual = (
        df_para_media_grupo_atual
        .groupby(nome_coluna)
        .mean()
    )
    
    # Renomeia colunas para MED_<VARIAVEL>
    df_medias_grupo_atual = df_medias_grupo_atual.rename(columns={col: f"MED_{col}" for col in df_medias_grupo_atual.columns})

    # Armazena em dicionário global (opcional)
    nome_df_medias = f"df_medias_{abrev_atual}"
    df_medias_por_grupo[nome_df_medias] = df_medias_grupo_atual
    
    # Exporta análise das médias
    fnc_tcc.exportar_dataframe_csv (df_medias_grupo_atual,diretorio_arquivos+"/rotina","cluterizacao_medias_indicadores_"+abrev_atual+".csv",True)  

	# Gera graficos das metricas
    graficos_gerados = fnc_tcc.gerar_grafico_metricas(df_metricas_grupo_atual, "Clusters "+nome_grupo_atual, abrev_atual, diretorio_imagens)
    
    # Filtra e ordena a lista de graficos gerados a utilizar no mosaico
    graficos_gerados_ordenados = fnc_tcc.filtrar_e_ordenar_graficos(graficos_gerados)
    
    # Gera Mosaico das metricas
    fnc_tcc.gerar_mosaico(graficos_gerados_ordenados
                          ,2
                         ,diretorio_imagens+'/'+ abrev_atual +'_metricas_consolidadas.png')

    # Gera Grafico unico com as metricas normalizadas utilizadas no Score e o Score
    fnc_tcc.gerar_grafico_metricas_unico(df_metricas_grupo_atual_norm[['k','Razao_Variancia', 'BIC','Silhouette_Score','Indice_Jaccard','Score']]
                                 , inicio_titulo = "Clusters "+nome_grupo_atual
                                 , caminho_destino = diretorio_imagens+'/'+ abrev_atual +'_metricas_norm_unicas.png'
                                 , normalizar = False
                                 , melhor_k = None)
    
    # Gera Grafico unico com as metricas normalizadas utilizadas no Score e o Score indicando o melhor k
    fnc_tcc.gerar_grafico_metricas_unico(df_metricas_grupo_atual_norm[['k','Razao_Variancia', 'BIC','Silhouette_Score','Indice_Jaccard','Score']]
                                 , inicio_titulo = "Clusters "+nome_grupo_atual
                                 , caminho_destino = diretorio_imagens+'/'+ abrev_atual +'_metricas_norm_unicas_melhor_k.png'
                                 , normalizar = False
                                 , melhor_k = melhor_k_grupo)
    
	# Gera o mapa de calor das metricas
    df_metricas_mapa_atual = df_metricas_grupo_atual.drop(columns=['Inercia'], errors='ignore').copy()
    fnc_tcc.gerar_mapacalor_indicadores(df_metricas_mapa_atual, "Mapa de Calor - Clusters "+nome_grupo_atual, diretorio_imagens+"/"+abrev_atual+"_mapa_calor.png")
    fnc_tcc.gerar_mapacalor_indicadores(df_metricas_mapa_atual, "Mapa de Calor - Clusters "+nome_grupo_atual, diretorio_imagens+"/"+abrev_atual+"_mapa_calor_com_destaque.png",melhor_k_grupo)
 
	# Exporta DataFrame consolidado com todos os clusters dos grupos
    fnc_tcc.exportar_dataframe_csv (df_metricas_grupo_atual,diretorio_arquivos+"/rotina",f"{nome_df_metricas}.csv")  
    fnc_tcc.exportar_dataframe_csv (df_metricas_grupo_atual_norm,diretorio_arquivos+"/rotina",f"{nome_df_metricas_norm}.csv")  

# Gera DataFrame com os melhores k por grupo de indicador
df_melhor_k_por_grupo = pd.DataFrame(ls_melhor_k_por_grupo)

# Exporta DataFrame consolidado com todos os clusters dos grupos
fnc_tcc.exportar_dataframe_csv (df_consolidado_variaveis_clusters,diretorio_arquivos+"/rotina","df_consolidado_variaveis_clusters.csv")
fnc_tcc.exportar_dataframe_csv (df_melhor_k_por_grupo,diretorio_arquivos+"/rotina","df_melhor_k_por_grupo.csv")  

#%% GERAR CLUSTERS FINAL BASEADO NO MELHORES CLUSTERS CRIADOS ANTERIORMENTE

# Se não localizar o df_consolidado_variaveis_clusters, realiza importação do backup gerado em arquivo
if not fnc_tcc.consultar_existencia_objeto('df_consolidado_variaveis_clusters', globals()):
    fnc_tcc.log("DataFrame df_consolidado_variaveis_clusters não existente, iniciando importação do backup")
    chek_arq, df_consolidado_variaveis = fnc_tcc.verificar_importar_arquivo_csv(diretorio_arquivos+"/rotina","df_consolidado_variaveis_clusters.csv",True,True)


# Copia DataFrame com o consolidado das variaveis
df_consolidado_variaveis_clusters_final = df_consolidado_variaveis_clusters.copy()

# Obtem as colunas que possuem os clusters por grupo
fnc_tcc.log("Obtendo nome das colunas que possuem os clusters por grupo")
colunas_clusters = df_consolidado_variaveis_clusters_final.filter(like='CLUSTER_').columns

# Gera DataFrame com as colunas dos Ks
fnc_tcc.log("Gerando dataframe com as colunas que possuem os clusters por grupo")
df_clusters_grupos = df_consolidado_variaveis_clusters_final[colunas_clusters].copy()
print(df_clusters_grupos.describe().T[['count', 'mean', 'std', 'min', 'max']].round(0))


# Executa Clusterização
fnc_tcc.log("Executando Clusterização do Final")
df_clusters_final, df_metricas_final = fnc_tcc.gerar_clusters_kmeans(df_clusters_grupos, k_values)


# Identifica o melhor K
melhor_k_final, df_metricas_final_norm = fnc_tcc.identificar_melhor_k(df_metricas_final)
fnc_tcc.log(f"Identificado o k '{melhor_k_final}' como o melhor 'k' para o Cluster Final")


# Recupera as métricasd do melhor k
metricas_melhor_k = df_metricas_final.loc[df_metricas_final['k'] == melhor_k_final].iloc[0]
 

# Empilha o melhor k
ls_melhor_k_por_grupo.append({
                         'GRUPO_INDICADOR': 'Final'
                         ,'MELHOR_K': melhor_k_final
                         ,'Razao_Variancia': metricas_melhor_k.get('Razao_Variancia')
                         ,'BIC': metricas_melhor_k.get('BIC')
                         ,'Silhouette_Score': metricas_melhor_k.get('Silhouette_Score')
                         ,'Indice_Jaccard': metricas_melhor_k.get('Indice_Jaccard')
                     })
 
# Gera/Atualiza DataFrame com os melhores k por grupo de indicador
df_melhor_k_por_grupo = pd.DataFrame(ls_melhor_k_por_grupo)
 

# Gera coluna com clusters do melhor k no df_consolidado_variaveis
nome_coluna = "CLUSTER_FINAL"
df_consolidado_variaveis_clusters_final[nome_coluna] = df_clusters_final[f'cluster_k{melhor_k_final}']
fnc_tcc.log(f"Criado a coluna '{nome_coluna}' no df_consolidado_variaveis_clusters_final")


nome_grupo_atual = "Final"
abrev_atual = 'final'


# Gera graficos das metricas
graficos_gerados = fnc_tcc.gerar_grafico_metricas(df_metricas_final, "Clusters "+nome_grupo_atual, abrev_atual, diretorio_imagens)


# Filtra e ordena a lista de graficos gerados a utilizar no mosaico
graficos_gerados_ordenados = fnc_tcc.filtrar_e_ordenar_graficos(graficos_gerados)


# Gera Mosaico das metricas
fnc_tcc.gerar_mosaico(graficos_gerados_ordenados
                      ,2
                     ,diretorio_imagens+'/'+ abrev_atual +'_metricas_consolidadas.png')


# Gera Grafico unico com as metricas normalizadas utilizadas no Score e o Score
fnc_tcc.gerar_grafico_metricas_unico(df_metricas_final_norm[['k','Razao_Variancia', 'BIC','Silhouette_Score','Indice_Jaccard','Score']]
                             , inicio_titulo = "Clusters "+nome_grupo_atual
                             , caminho_destino = diretorio_imagens+'/'+ abrev_atual +'_metricas_norm_unicas.png'
                             , normalizar = False
                             , melhor_k = None)


# Gera Grafico unico com as metricas normalizadas utilizadas no Score e o Score com indicação do melhor k
fnc_tcc.gerar_grafico_metricas_unico(df_metricas_final_norm[['k','Razao_Variancia', 'BIC','Silhouette_Score','Indice_Jaccard','Score']]
                             , inicio_titulo = "Clusters "+nome_grupo_atual
                             , caminho_destino = diretorio_imagens+'/'+ abrev_atual +'_metricas_norm_unicas_melhor_k.png'
                             , normalizar = False
    
                             , melhor_k = melhor_k_final)
# Gera o mapa de calor
df_metricas_final_mapa_atual = df_metricas_final.drop(columns=['Inercia'], errors='ignore').copy()
fnc_tcc.gerar_mapacalor_indicadores(df_metricas_final_mapa_atual, "Mapa de Calor - Clusters "+nome_grupo_atual, diretorio_imagens+"/"+abrev_atual+"_mapa_calor.png")
fnc_tcc.gerar_mapacalor_indicadores(df_metricas_final_mapa_atual, "Mapa de Calor - Clusters "+nome_grupo_atual, diretorio_imagens+"/"+abrev_atual+"_mapa_calor_com_destaque.png",melhor_k_final)


# Exporta DataFrame consolidado com todos os clusters dos grupos
fnc_tcc.exportar_dataframe_csv (df_metricas_final,diretorio_arquivos+"/rotina","df_metricas_final.csv")  
fnc_tcc.exportar_dataframe_csv (df_metricas_final_norm,diretorio_arquivos+"/rotina","df_metricas_final_norm.csv")  


# Atualiza a lista de campos de clusters
colunas_clusters_finais = [
    c for c in df_consolidado_variaveis_clusters_final.columns
    if c.startswith('CLUSTER_')
]


# Avaliando os clusters finais
print(df_consolidado_variaveis_clusters_final[colunas_clusters_finais].describe().T[['count', 'mean', 'std', 'min', 'max']].round(2))


# Exporta DataFrame consolidado com todos os detalhes e clusters
fnc_tcc.exportar_dataframe_csv (df_consolidado_variaveis_clusters_final,diretorio_arquivos+"/rotina","df_consolidado_variaveis_clusters_final.csv")  


# Exporta as analises descrivas
fnc_tcc.exportar_dataframe_csv (pd.DataFrame(df_consolidado_variaveis_clusters_final.describe().T),diretorio_arquivos+"/rotina","describe_df_consolidado_variaveis_clusters_final.csv",True)  


# Configura para que o pandad exiba todas as colunas e linhas completas
pd.set_option('display.max_columns', None)  # Mostra todas as colunas
pd.set_option('display.max_rows', None)     # Mostra todas as linhas
pd.set_option('display.width', None)        # Ajusta a largura total
pd.set_option('display.max_colwidth', None) # Mostra colunas completas (sem truncar texto)


# Exibe analise descritiva para simples consulta
print(df_consolidado_variaveis_clusters_final.describe().T[['count', 'mean', 'std', 'min', 'max']].round(2))


# Reseta as configuracoes do pandas
pd.reset_option('all')


#%% GERA TABELA COM TODAS VARIAVEIS PARA ANALISES E DASHBOARDS

"""
df_variaveis_municipios # variaveis originais
df_variaveis_normalizadas # variaveis normalizadas
df_consolidado_variaveis_clusters # variaveis originais + normalizadas + clusters (geral,popul,educ,saude,)

colunas = df_consolidado_variaveis_clusters. columns

"""

"""

ID_LOCALIDADE
NOME_LOCALIDADE
TIPO_DADO (VARIAVEL, CLUSTER, VARIAVEL NORMALIZADA, RANK)
GRUPO_DADO (DADOS GERAIS, POPULACAO, EDUCACAO)
REFERENCIA_DADO (VAR_01, CLUSTER_SAUDE, VAR_01, VAR_01)
DESCRICAO_DADO (DESCRICAO DA VARIAVEL, CLUSTER SAUDE, RANK 'DESCRICAO VARIAVEL')
VALOR (VALOR DA VARIAVEL, CLUSTER, VALOR DA VARIAVEL NORMALIZADA, POSICAO NO RANK)
UNIDADE_MEDIDA (%, unidade, pessoas, º)


"""



    