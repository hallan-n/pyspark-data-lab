import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count
from domain import LayerFlow

class Gold(LayerFlow):
    def __init__(self):
        self.spark = SparkSession.builder.appName("CamadaGold").getOrCreate()

    def run(self):
        """Analisa os dados tratados"""
        silver_path = "app/silver/clientes_compras/"

        # Verificação de existência
        if not os.path.exists(silver_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {silver_path}")

        try:
            print("🔄 Lendo dados da camada Silver...")
            df = self.spark.read.parquet(silver_path)
        except Exception as e:
            raise RuntimeError(f"Erro ao ler os dados da camada Silver: {e}")

        try:
            print("📊 Gerando agregações...")
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
        except Exception as e:
            raise RuntimeError(f"Erro ao transformar os dados: {e}")

        try:
            print("💾 Gravando dados agregados na camada Gold...")
            gastos_por_cliente.write.mode("overwrite").parquet("app/gold/gastos_por_cliente/")
            media_por_cidade.write.mode("overwrite").parquet("app/gold/media_por_cidade/")
            produtos_mais_vendidos.write.mode("overwrite").parquet("app/gold/produtos_mais_vendidos/")
        except Exception as e:
            raise RuntimeError(f"Erro ao gravar os dados na camada Gold: {e}")

        print("✅ Camada Gold gerada com sucesso.")

    def show(self):
        """Mostra os dados analisados"""
        path_gastos = "app/gold/gastos_por_cliente/"
        path_media = "app/gold/media_por_cidade/"
        path_produtos = "app/gold/produtos_mais_vendidos/"

        try:
            df_gastos = self.spark.read.parquet(path_gastos)
            df_media = self.spark.read.parquet(path_media)
            df_produtos = self.spark.read.parquet(path_produtos)

            print("\n📊 Total gasto por cliente:")
            df_gastos.show(truncate=False)

            print("\n🏙️ Média de valor por cidade:")
            df_media.show(truncate=False)

            print("\n🛒 Produtos mais vendidos:")
            df_produtos.show(truncate=False)
        except Exception as e:
            raise RuntimeError(f"Erro ao mostrar os dados da camada Gold: {e}")
