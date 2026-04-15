# 📊 Clusterização de Municípios Brasileiros com Dados do IBGE
[![Python](https://img.shields.io/badge/-Python-3776AB?logo=python&logoColor=white)](#)
[![Pandas](https://img.shields.io/badge/-Pandas-150458?logo=pandas&logoColor=white)](#)
[![NumPy](https://img.shields.io/badge/-NumPy-013243?logo=numpy&logoColor=white)](#)
[![Scikit-learn](https://img.shields.io/badge/-Scikit--learn-F7931E?logo=scikitlearn&logoColor=white)](#)
[![Matplotlib](https://img.shields.io/badge/-Matplotlib-11557C)](#)
[![Seaborn](https://img.shields.io/badge/-Seaborn-4C72B0)](#)


**Autor:** <a href="https://linkedin.com/in/evertonsmoraes/" target="_blank">Everton S. Moraes</a></br>


Projeto desenvolvido como Trabalho de Conclusão de Curso (TCC) no MBA em Data Science & Analytics (USP/ESALQ), com o objetivo de agrupar municípios brasileiros a partir de indicadores socioeconômicos públicos, utilizando técnicas de Machine Learning não supervisionado.

🚀 A solução permite identificar perfis municipais semelhantes, apoiando análises comparativas (benchmarking) e contribuindo para a tomada de decisão na gestão pública.


## 🎯 Objetivo Geral

Aplicar o algoritmo K-means para agrupar municípios brasileiros com base em indicadores socioeconômicos públicos, visando identificar perfis semelhantes que possam subsidiar práticas de benchmarking na gestão pública.



## 🎯 Objetivos Específicos

1. Desenvolver agrupamentos de municípios com base em diferentes grupos de indicadores:
    - 📍 Territoriais
    - 👥 Populacionais
    - 🎓 Educacionais
    - 🏥 Sociais e de saúde    
    - 💰 Econômicos
    - 🏗️ Estruturais 
2. Avaliar a qualidade dos agrupamentos utilizando múltiplas métricas de validação:
    - Razão das Variâncias (VRC)  
    - Critério de Silhueta  
    - Índice de Jaccard  
    - Bayesian Information Criterion (BIC)
3. Avaliar a viabilidade de integração dos agrupamentos em um modelo geral de similaridade municipal.



## 📊 Dados Utilizados

- Fonte: Instituto Brasileiro de Geografia e Estatística (IBGE)
- Coleta via API pública
- Abrangência: 5.570 municípios brasileiros
- Total de indicadores utilizados: 38



## 🧠 Metodologia
O processo metodológico foi estruturado nas seguintes etapas:

1. Extração dos dados via API do IBGE  
2. Tratamento e estruturação das bases  
3. Padronização das variáveis utilizando Z-score  
4. Aplicação do algoritmo K-means por grupo de indicadores  
5. Teste de múltiplos valores de k (100 a 500, com incremento de 5)  
6. Avaliação da qualidade dos agrupamentos por métricas de validação  
7. Construção de um score ponderado para definição do melhor k  
8. Análise comparativa dos agrupamentos obtidos


## ## 📈 Exemplo Prático: Análise do Grupo "População"

### Evolução das métricas ao longo de k
![Métricas por k](./imgs/popul_metricas_consolidadas.png)
Este gráfico demonstra o comportamento das métricas ao longo dos diferentes valores de k, evidenciando que o aumento do número de clusters não implica necessariamente melhoria na qualidade estrutural dos agrupamentos.


###  Definição do melhor número de clusters (Score combinado)
![Score](./imgs/popul_metricas_norm_unicas_melhor_k.png)
As métricas foram normalizadas e combinadas em um score ponderado, permitindo identificar de forma mais robusta o número ótimo de clusters.



## 📊 Métricas de Avaliação
Foram utilizadas múltiplas métricas para garantir uma avaliação robusta dos clusters:

- **Critério de Silhueta:** Avalia a coesão interna e a separação entre clusters  
- **Razão das Variâncias (VRC):** Mede a relação entre dispersão intra e inter-clusters  
- **Índice Jaccard:** Avalia a estabilidade dos agrupamentos  
- **BIC (Bayesian Information Criterion):**  Penaliza a complexidade do modelo  

📌 **Diferencial do projeto:** As métricas foram normalizadas e combinadas em um **score ponderado**, permitindo uma decisão mais consistente na escolha do número ótimo de clusters.


## 🔁 Reprodução
O projeto foi estruturado para permitir a reconstrução completa dos dados a partir do código, garantindo reprodução dos resultados.

### O pipeline contempla:

1. Coleta automática via API  
2. Tratamento e padronização dos dados  
3. Modelagem e avaliação dos clusters  
4. Geração de resultados e visualizações  


## 📁 Estrutura do Projeto

```bash
.
Projeto_TCC/
├── funcoes_tcc.py         # Biblioteca central do projeto, responsável por concentrar funções reutilizáveis.
├── 00_discovery.py        # Script de análise exploratória (discovery) para a coleta, analise e definição dos indicadores a serem utilizados no projeto.
├── 01_desenvolvimento.py  # Script com todas as etapads do projeto desde a coleta de dados aos resultados
├── arquivos/                   
│   ├── entrada/            # Arquivos de entrada (lista de municipios brasileiros, indicadores selecionados ,etc)
│   ├── rotina/             # Arquivos gerados durante execução da(s) rotina(s)
│   ├── saida/              # Arquivos de resultados e outputs gerados durante execução da(s) rotina(s)
├── imgs/                   # Diretório com as imagens, gráficos, tabelas geradas durante as execuções da(s) rotina(s) 
├── .spyproject/            # Diretório criado automaticamente pelo ambiente de desenvolvimento integrado (IDE) Spyder 
├── README.md               # Documento de introdução e documentação a este projeto
