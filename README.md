# BrasilFlow: Modern Data Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=Apache-Spark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

## Visão Geral
O **BrasilFlow** é um pipeline de dados desenvolvido para processar e analisar informações de mobilidade urbana da cidade de São Paulo (SPTrans). O projeto implementa uma arquitetura de *Data Lakehouse* robusta, unificando processamento em tempo real e em lote para fornecer uma fundação analítica escalável.

O pipeline consome posições de GPS de ônibus via streaming, integrando essas informações com dados relacionais operacionais (motoristas, escalas e manutenções) e malhas de transporte (GTFS) para modelagem analítica e suporte à tomada de decisão.

## Arquitetura de Dados

O projeto foi desenhado sob os princípios da **Arquitetura Medallion**, garantindo qualidade, rastreabilidade e performance ao longo do ciclo de vida dos dados.

```mermaid
graph TD
    subgraph Fontes de Dados
        A[API SPTrans GPS] -->|JSON / Streaming| B(AWS S3)
        C[SQL Server RDS] -->|JDBC / Batch| D(Databricks)
        E[CSV GTFS] -->|Batch| F(AWS S3)
    end

    subgraph Databricks Lakehouse
        B --> G[(Camada Bronze)]
        D --> G
        F --> G

        G --> H[(Camada Silver)]
        
        H --> I[(Camada Gold)]
    end

    subgraph Consumo
        I --> J[Ferramentas de BI]
        I --> K[Análises Ad-Hoc]
    end
```

## Estrutura das Camadas

*   **Camada Bronze (Raw Data):** Ingestão de dados brutos provenientes de fontes heterogêneas. Utiliza processamento em streaming para consumo contínuo da API de GPS e integração via JDBC para a replicação de dados transacionais.
*   **Camada Silver (Cleansed Data):** Camada de integração e qualidade. Os dados passam por processos de limpeza, padronização e validação. Anomalias e registros corrompidos são tratados para assegurar a consistência dos dados que alimentarão as análises.
*   **Camada Gold (Curated Data):** Modelagem dimensional estruturada em *Star Schema* (Tabelas Fato e Dimensões). Os dados são enriquecidos e otimizados para alta performance em consultas analíticas e construção de dashboards corporativos.

## Stack Tecnológico

*   **Cloud Provider:** AWS (Amazon S3, Amazon RDS)
*   **Data Processing Engine:** Databricks, Apache Spark (PySpark)
*   **Armazenamento Analítico:** Delta Lake
*   **Modelagem e Transformação:** SQL, Python
*   **Orquestração de Pipelines:** Databricks Workflows
*   **Governança de Dados:** Unity Catalog
