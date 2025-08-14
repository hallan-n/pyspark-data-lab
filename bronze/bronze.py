from pyspark.sql import SparkSession
from domain import LayerFlow


class Bronze(LayerFlow):
    def __init__(self):
        self.spark = SparkSession.builder.appName("IngestaoBronze").getOrCreate()\

    def run(self):
        clientes_csv = "bronze/clientes/clientes.csv"
        compras_csv = "bronze/compras/compras.csv"
        clientes_df = self.spark.read.csv(clientes_csv, header=True, inferSchema=True)
        compras_df = self.spark.read.csv(compras_csv, header=True, inferSchema=True)
        clientes_df.write.mode("overwrite").parquet("bronze/parquet/clientes/")
        compras_df.write.mode("overwrite").parquet("bronze/parquet/compras/")
        print("Ingestão concluída com sucesso.")

    def show(self):
        clientes_parquet = "bronze/parquet/clientes/"
        compras_parquet = "bronze/parquet/compras/"
        clientes_df = self.spark.read.parquet(clientes_parquet)
        compras_df = self.spark.read.parquet(compras_parquet)
        clientes_df.show()
        compras_df.show()


bronze = Bronze()

if __name__ == "__main__":
    bronze.run()
    bronze.show()