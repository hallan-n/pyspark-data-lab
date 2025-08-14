import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from domain import LayerFlow

class Silver(LayerFlow):
    def __init__(self):
        self.spark = SparkSession.builder.appName("CamadaSilver").getOrCreate()

    def run(self):
        """Faz a tratativa dos dados brutos"""
        clientes_path = "app/bronze/parquet/clientes/"
        compras_path = "app/bronze/parquet/compras/"

        # Verificações de existência
        if not os.path.exists(clientes_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {clientes_path}")
        if not os.path.exists(compras_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {compras_path}")

        try:
            print("🔄 Lendo arquivos Parquet...")
            clientes_df = self.spark.read.parquet(clientes_path)
            compras_df = self.spark.read.parquet(compras_path)
        except Exception as e:
            raise RuntimeError(f"Erro ao ler os arquivos Parquet: {e}")

        try:
            print("🧹 Tratando dados...")
            clientes_df = clientes_df.dropna()
            compras_df = compras_df.dropna()
            compras_df = compras_df.withColumn("data", to_date(col("data"), "yyyy-MM-dd"))
            clientes_compras_df = compras_df.join(clientes_df, on="cliente_id", how="inner")
        except Exception as e:
            raise RuntimeError(f"Erro ao transformar os dados: {e}")

        try:
            print("💾 Gravando dados tratados...")
            clientes_df.write.mode("overwrite").parquet("app/silver/clientes/")
            compras_df.write.mode("overwrite").parquet("app/silver/compras/")
            clientes_compras_df.write.mode("overwrite").parquet("app/silver/clientes_compras/")
        except Exception as e:
            raise RuntimeError(f"Erro ao gravar os arquivos Parquet: {e}")

        print("✅ Transformação concluída com sucesso e dados salvos na camada Silver.")

    def show(self):
        """Mostra os dados tratados"""
        silver_path = "app/silver/clientes_compras/"
        try:
            df = self.spark.read.parquet(silver_path)
            df.show(truncate=False)
            df.printSchema()
        except Exception as e:
            raise RuntimeError(f"Erro ao mostrar os dados da camada Silver: {e}")
