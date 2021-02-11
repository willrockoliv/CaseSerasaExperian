# Twitter API Stream Listener using Python, Pyspark and Tweepy

## 1. O Case:

### O case consiste basicamente em implementar uma solução que: 
- Consiga buscar tweets com uma determinada “HashTag”, por exemplo, covid19;
- Armazenar os resultados em formato Parquet;
- Criar um Data Warehouse para que seja possível consolidar dados analíticos por hashtag e posteriormente consultar informações coletadas de forma batch/online.

### Recomendamos a utilização das seguintes tecnologias:
- Spark – Para extração, escrita e consolidação;
- Airflow – Orquestração do processamento;
- Docker/Compose – Para deploy da aplicação;

### Testes/Tópicos consideramos importantes:
- Organização;
- Testes automatizados;
- Boas práticas de Data Engineering;
- Arquitetura;
- Solução para Deploy em produção;
- Criatividade.

---

## 2. Solução:

A aplicação foi desenvolvida em Python, utilizando [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html) 
e a biblioteca [**Tweepy**](https://www.tweepy.org/), uma biblioteca para Python que facilita a conexão com a API do Twitter.

A aplicação está dividida em duas etapas:

**1º Etapa:** Realizar o streaming de tweets em tempo real, sendo esse o código do arquivo [2.TwitterAPIListener.py](https://github.com/willrockoliv/CaseSerasaExperian/blob/master/2.TwitterAPIListener.py),
onde dado uma lista de *keywords* a serem buscados em tweets, a aplicação irá:
- Realizar a conexão com a API de streaming do Twitter;
- Baixar os tweets em tempo real
- Salvá-los em uma *layer1* de dados de forma particionada e em arquivos no formato parquet.

**2º Etapa:** Criar um Data Warehouse com os dados salvos na *layer1*, sendo esse o código [3.DataWarehouse.py](https://github.com/willrockoliv/CaseSerasaExperian/blob/master/3.DataWarehouse.py),
no qual sua função é a cada 60 minutos:
- Ler os dados salvos na *layer1* na última hora;
- Realizar os devidos tratamentos dos dados como:
  - Cast das colunas para seus respectivos tipos de dados, `Integer`, `Boolean`, `Timestamp` e `String`;
  - Ajuste de fuso horário das colunas de timestamp para "America/Sao_Paulo";
- Criar as tabelas fato e dimensão;
- Salvá-las de forma particionada e em formato parquet numa *layer2* de dados.

## 3. O que falta:

- A implementação de um orquestrador do fluxo de dados, como por exemplo o Airflow sugerido no case
- A criação de um ambiente Docker/Compose para deploy da aplicação

## 4. O que era pretendido incluir:
- Armazenamento na Amazon S3

## 5. Arquitetura pretendida:
![Arquitetura](https://github.com/willrockoliv/CaseSerasaExperian/blob/master/Arquitetura.png)

