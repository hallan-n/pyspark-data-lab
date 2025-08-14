from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

from domain import LayerFlow

class Silver(LayerFlow):
    def __init__(self):
        self.spark = SparkSession.builder.appName("CamadaSilver").getOrCreate()

    def run(self):
        clientes_path = "bronze/parquet/clientes/"
        compras_path = "bronze/parquet/compras/"
        clientes_df = self.spark.read.parquet(clientes_path)
        compras_df = self.spark.read.parquet(compras_path)
        clientes_df = clientes_df.dropna()
        compras_df = compras_df.dropna()
        compras_df = compras_df.withColumn("data", to_date(col("data"), "yyyy-MM-dd"))
        clientes_compras_df = compras_df.join(clientes_df, on="cliente_id", how="inner")
        clientes_df.write.mode("overwrite").parquet("silver/clientes/")
        compras_df.write.mode("overwrite").parquet("silver/compras/")
        clientes_compras_df.write.mode("overwrite").parquet("silver/clientes_compras/")
        print("Transformação concluída com sucesso e dados salvos na camada Silver.")

    def show(self):  
        silver_path = "silver/clientes_compras/"
        df = self.spark.read.parquet(silver_path)
        df.show(truncate=False)
        df.printSchema()

silver = Silver()

if __name__ == "__main__":
    silver.run()
    silver.show()