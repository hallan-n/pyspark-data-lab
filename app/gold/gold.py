from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count
from domain import LayerFlow

class Gold(LayerFlow):
    def __init__(self):
        self.spark = SparkSession.builder.appName("CamadaGold").getOrCreate()

    def run(self):
        """Analisa os dados tratados"""
        silver_path = "app/silver/clientes_compras/"
        df = self.spark.read.parquet(silver_path)
        gastos_por_cliente = df.groupBy("cliente_id", "nome").agg(
            sum("valor").alias("total_gasto"),
            count("compra_id").alias("qtd_compras")
        )
        media_por_cidade = df.groupBy("cidade").agg(
            avg("valor").alias("media_valor")
        )
        produtos_mais_vendidos = df.groupBy("produto").agg(
            count("compra_id").alias("qtd_vendas")
        ).orderBy("qtd_vendas", ascending=False)
        gastos_por_cliente.write.mode("overwrite").parquet("app/gold/gastos_por_cliente/")
        media_por_cidade.write.mode("overwrite").parquet("app/gold/media_por_cidade/")
        produtos_mais_vendidos.write.mode("overwrite").parquet("app/gold/produtos_mais_vendidos/")
        print("Camada Gold gerada com sucesso.")

    def show(self):
        """Mostra os dados analisados"""
        path_gastos = "app/gold/gastos_por_cliente/"
        path_media = "app/gold/media_por_cidade/"
        path_produtos = "app/gold/produtos_mais_vendidos/"
        df_gastos = self.spark.read.parquet(path_gastos)
        df_media = self.spark.read.parquet(path_media)
        df_produtos = self.spark.read.parquet(path_produtos)
        print("\n📊 Total gasto por cliente:")
        df_gastos.show(truncate=False)
        print("\n🏙️ Média de valor por cidade:")
        df_media.show(truncate=False)
        print("\n🛒 Produtos mais vendidos:")
        df_produtos.show(truncate=False)


gold = Gold()

if __name__ == "__main__":
    gold.run()
    gold.show()