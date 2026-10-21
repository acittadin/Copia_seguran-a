# projeto_pesquisa_spark_parte3

## Como rodar localmente

1. Entre na pasta do projeto:
   ```bash
   cd /home/andrecittadin/projeto_spark3/projeto_pesquisa_spark_parte3
   ```
2. Ative o ambiente virtual:
   ```bash
   . .venv/bin/activate
   ```
3. Execute o pipeline:
   ```bash
   python run_local_pipeline.py
   ```

## Requisitos
- Python 3.11
- Java 17
- Dependências instaladas no ambiente virtual:
  - pyspark==3.5.3
  - delta-spark==3.2.1
  - pandas

## Observação
Este projeto foi ajustado para execução local em modo Spark, sem depender de placeholders de Databricks. O job em JSON continua sendo um artefato de orquestração, mas o pipeline local está pronto para execução direta.