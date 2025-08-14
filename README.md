# pyspark-data-lab

Projeto de laboratório de dados utilizando PySpark, estruturado em camadas (bronze, silver, gold) para ingestão, processamento e disponibilização de dados.

## 📂 Estrutura do Projeto

```
main.py
pyproject.toml
bronze/
  clientes/
    clientes.csv
    clientes.json
    clientes.orc
    clientes.parquet
  compras/
    compras.csv
    compras.json
    compras.orc
    compras.parquet
gold/
  clientes_compras/
  dashboards/
silver/
  clientes/
  compras/
```

## 🔄 Fluxo das Camadas

### Bronze
- **Objetivo:** Armazenar dados brutos, exatamente como recebidos das fontes.
- **Exemplo:** Arquivos CSV, JSON, Parquet, ORC.

### Silver
- **Objetivo:** Armazenar dados limpos e estruturados, prontos para análises intermediárias.
- **Exemplo:** Dados tratados, padronizados e sem duplicidades.

### Gold
- **Objetivo:** Disponibilizar dados prontos para consumo analítico, dashboards e relatórios.
- **Exemplo:** Tabelas finais, datasets para BI, dashboards.

## 🚀 Como Executar

1. Instale as dependências (recomenda-se Poetry):
   ```sh
   poetry install
   ```
2. Ative o ambiente virtual:
   ```sh
   poetry env activate
   # ou
   source $(poetry env info --path)/bin/activate
   ```
3. Execute os scripts desejados:
   ```sh
   #Rodar cada camada individual
   poetry run python3 bronze/bronze.py
   poetry run python3 silver/silver.py
   poetry run python3 gold/gold.py
   
   # Roda fluxo completo de todas as camadas
   poetry run python3 main.py
   ```

## 📝 Observações
- Certifique-se de ter o Java 17 instalado e configurado no PATH para rodar PySpark.
- Os arquivos de dados das camadas bronze, silver e gold não devem ser versionados (veja `.gitignore`).
- Scripts e notebooks podem ser versionados normalmente.

## 📄 Licença
Este projeto é apenas para fins educacionais e laboratoriais.
