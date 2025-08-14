from pyspark.sql import SparkSession
from domain import LayerFlow
import os


class Bronze(LayerFlow):
    def __init__(self):
        self.spark = SparkSession.builder.appName("IngestaoBronze").getOrCreate()\

import os
from pyspark.sql import SparkSession

class Bronze:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CamadaBronze").getOrCreate()

    def run(self):
        """Realiza a ingestão dos dados"""
        clientes_csv = "app/bronze/clientes/clientes.csv"
        compras_csv = "app/bronze/compras/compras.csv"

        if not os.path.exists(clientes_csv):
            raise FileNotFoundError(f"Arquivo não encontrado: {clientes_csv}")
        if not os.path.exists(compras_csv):
            raise FileNotFoundError(f"Arquivo não encontrado: {compras_csv}")

        try:
            print("🔄 Lendo arquivos CSV...")
            clientes_df = self.spark.read.csv(clientes_csv, header=True, inferSchema=True)
            compras_df = self.spark.read.csv(compras_csv, header=True, inferSchema=True)
        except Exception as e:
            raise RuntimeError(f"Erro ao ler os arquivos CSV: {e}")

        try:
            clientes_df.write.mode("overwrite").parquet("app/bronze/parquet/clientes/")
            compras_df.write.mode("overwrite").parquet("app/bronze/parquet/compras/")
        except Exception as e:
            raise RuntimeError(f"Erro ao gravar os arquivos Parquet: {e}")

        print("✅ Ingestão concluída com sucesso.")


    def show(self):
        """Mostra os dados ingeridos"""
        clientes_parquet = "app/bronze/parquet/clientes/"
        compras_parquet = "app/bronze/parquet/compras/"
        clientes_df = self.spark.read.parquet(clientes_parquet)
        compras_df = self.spark.read.parquet(compras_parquet)
        clientes_df.show()
        compras_df.show()


bronze = Bronze()

if __name__ == "__main__":
    bronze.run()
    bronze.show()