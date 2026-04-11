# Clusterização de Municípios Brasileiros com Dados do IBGE
Repositório contendo todos os códigos, análises e desenvolvimentos do Trabalho de Conclusão de Curso (TCC) em Data Science e Analytics, com foco na clusterização de municípios brasileiros a partir de indicadores socioeconômicos públicos disponibilizados pelo IBGE.

**Autor:** <a href="https://linkedin.com/in/evertonsmoraes/" target="_blank">Everton S. Moraes</a>
<br>


# Detalhes do Projeto


### 🎯 Objetivo
Este projeto tem como objetivo aplicar técnicas de **Machine Learning não supervisionado** para identificar padrões e agrupar municípios brasileiros com características socioeconômicas semelhantes, apoiando análises comparativas e tomadas de decisão.
</br>

### ⚙️ Etapas
1. Coleta de dados via API do IBGE
2. Tratamento e padronização de dados
3. Criação de variáveis analíticas
4. Análise das variáveis analíticas
5. Padronização das Variáveis
6. Aplicação de algoritmos de clusterização (K-Means e GMM)
7. Avaliação de clusters com múltiplas métricas
8. Geração de visualizações e análises exploratórias
9. Indentificado o melhor *k* para as clusterizações geradas

### 🖥️ Tecnologias utilizadas
  - Python
  - Pandas / NumPy
  - Scikit-learn
  - Matplotlib / Seaborn
  - API IBGE

# Reprodução do Projeto
O projeto foi estruturado para permitir a reconstrução completa dos dados a partir do código

### 🔁 Para reproduzir os dados:
1. Execute os scripts de coleta
2. Execute as rotinas de tratamento
3. Execute os módulos de clusterização
   
### 📜 Licença
Este projeto está licenciado sob os termos da Licença MIT.

# Sobre os dados do projeto
⚠️ Devido ao volume dos dados utilizados no projeto, nem todos os arquivos estão versionados neste repositório.
Em especial:
- Arquivos da pasta *arquivos/rotina/*
- Arquivos da pasta *arquivos/saida/*
- Bases muito grandes (ex: dados completos municipais)

### ℹ️ Esses arquivos são:
- Gerados automaticamente durante a execução dos scripts
- Obtidos via API pública do IBGE
- Ou podem ser reconstruídos a partir do pipeline do projeto


# 📁 Estrutura do Projeto

```bash
Projeto_TCC/
│
├── funcoes_tcc.py         # Biblioteca central do projeto, responsável por concentrar funções reutilizáveis.
├── 00_discovery.py        # Script de análise exploratória (discorevy) para a coleta, analise e definição dos indicadores a serem utilizados no projeto.
├── 01_desenvolvimento.py  # Script com todas as etapads do projeto desde a coleta de dados aos resultados
├── arquivos/                   
│   ├── entrada/            # Arquivos de entrada (lista de municipios brasileiros, indicadores selecionados ,etc)
│   ├── rotina/             # Arquivos gerados durante execução da(s) rotina(s)
│   ├── saida/              # Arquivos de resultados e outputs gerados durante execução da(s) rotina(s)
├── imgs/                   # Diretório com as imagens, gráficos, tabelas geradas durante as execuções da(s) rotina(s) 
├── .spyproject/            # Diretório criado automaticamente pelo ambiente de desenvolvimento integrado (IDE) Spyder 
├── README.md               # Documento de introdução e documentação a este projeto
