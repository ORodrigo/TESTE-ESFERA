# BIBLIOTECAS

import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim, upper, when, regexp_replace, format_string, round
from pyspark.sql.types import StringType

# CONEXAO COM O POSTGRE NO DOCKER

# Definir variáveis de ambiente para o Hadoop/YARN
os.environ["HADOOP_CONF_DIR"] = "/path/to/hadoop/config"
os.environ["YARN_CONF_DIR"] = "/path/to/yarn/config"


spark = SparkSession.builder \
    .appName("PostgreSQL Integration") \
    .config("spark.jars", "/opt/airflow/scripts/postgresql-42.7.4.jar") \
    .master("yarn") \
    .getOrCreate()

# Definindo as configurações de conexão
url = "jdbc:postgresql://172.19.0.2:5432/postgres"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Inicializar SparkSession
spark = SparkSession.builder.appName("Orcamento-SP-2019").getOrCreate()

# Leitura dos arquivos CSV com charset UTF-8
df_despesas_bronze = spark.read.option("header", "true").option("charset", "UTF-8").csv("/opt/airflow/scripts/gdvDespesasExcel.csv")
df_receitas_bronze = spark.read.option("header", "true").option("charset", "UTF-8").csv("/opt/airflow/scripts/gdvReceitasExcel.csv")

# Função para remover caracteres especiais dos nomes das colunas
def clean_column_name(col_name):
    return re.sub(r'[^A-Za-z0-9_]+', '_', col_name)  # Substitui qualquer caractere não alfanumérico por '_'

# Aplicando a função de limpeza nos nomes das colunas
df_despesas_bronze = df_despesas_bronze.toDF(*[clean_column_name(col_name) for col_name in df_despesas_bronze.columns])
df_receitas_bronze = df_receitas_bronze.toDF(*[clean_column_name(col_name) for col_name in df_receitas_bronze.columns])

# Verificar se os nomes das colunas foram atualizados corretamente
print("Nomes das colunas - Despesas:", df_despesas_bronze.columns)
print("Nomes das colunas - Receitas:", df_receitas_bronze.columns)

# CAMADA BRONZE

df_despesas_bronze.show(5, truncate=False)
df_receitas_bronze.show(5, truncate=False)

#Armazenando camada bronze no banco de dados

#df_receitas_bronze.write.jdbc(url=url, table="receita_bronze", mode="append", properties=properties)
#df_despesas_bronze.write.jdbc(url=url, table="despesa_bronze", mode="append", properties=properties)

# CAMADA SILVER
# Trantando carcteres especiais no campo de texto. Tratanto valor do campo 'Liquidado' e 'Arrecadado_at_02_02_2024' para não transformar a informação como numero cientifo, mantendo o valor real. Retirando o campo '_c3', onde não há informação, apenas valores null

from pyspark.sql.functions import regexp_replace

# CAMADA SILVER

# Remover caracteres especiais da coluna 'Despesa' na df_despesas_bronze
df_despesas_silver = df_despesas_bronze.withColumn("Despesa", regexp_replace("Despesa", r'[^A-Za-z0-9 ]', ''))

# Remover caracteres especiais da coluna 'Receita' na df_receitas_bronze
df_receitas_silver = df_receitas_bronze.withColumn("Receita", regexp_replace("Receita", r'[^A-Za-z0-9 ]', ''))

# Excluir a coluna '_c3' em ambos os DataFrames
df_despesas_silver = df_despesas_silver.drop("_c3")
df_receitas_silver = df_receitas_silver.drop("_c3")

# Remover registros com valores nulos na coluna 'Fonte_de_Recursos'
df_despesas_silver = df_despesas_silver.filter(df_despesas_silver["Fonte_de_Recursos"].isNotNull())
df_receitas_silver = df_receitas_silver.filter(df_receitas_silver["Fonte_de_Recursos"].isNotNull())

# Verificar as primeiras linhas para garantir que as transformações foram aplicadas
df_despesas_silver.show(5, truncate=False)
df_receitas_silver.show(5, truncate=False)



#Armazenando camada Silver no banco de dados

#df_despesas_silver.write.jdbc(url=url, table="despesa_silver", mode="append", properties=properties)
#df_receitas_silver.write.jdbc(url=url, table="receita_silver", mode="append", properties=properties)

# CAMADA GOLD

from pyspark.sql.functions import col, regexp_replace, sum, format_number


# Remover caracteres especiais da coluna 'Despesa' na df_despesas_bronze
df_despesas_silver = df_despesas_bronze.withColumn("Despesa", regexp_replace("Despesa", r'[^A-Za-z0-9 ]', ''))

# Remover caracteres especiais da coluna 'Receita' na df_receitas_bronze
df_receitas_silver = df_receitas_bronze.withColumn("Receita", regexp_replace("Receita", r'[^A-Za-z0-9 ]', ''))

# Excluir a coluna '_c3' em ambos os DataFrames
df_despesas_silver = df_despesas_silver.drop("_c3")
df_receitas_silver = df_receitas_silver.drop("_c3")

# Tratamento do campo 'Liquidado' no df_despesas_silver
df_despesas_silver = df_despesas_silver.withColumn(
    "Liquidado", 
    regexp_replace(col("Liquidado"), r'\.', '')  # Remove pontos (milhares)
)

df_despesas_silver = df_despesas_silver.withColumn(
    "Liquidado", 
    regexp_replace(col("Liquidado"), r',', '.')  # Substitui vírgula por ponto
)

# Converter para tipo inteiro (sem casas decimais)
df_despesas_silver = df_despesas_silver.withColumn(
    "Liquidado", col("Liquidado").cast("long")  # 'long' para números inteiros
)

# Agrupar por 'Fonte_de_Recursos' e somar a coluna 'Liquidado' no df_despesas_silver
df_despesas_gold = df_despesas_silver.groupBy("Fonte_de_Recursos").agg(
    sum("Liquidado").alias("Total_Liquidado")
)

# Filtrar registros com Fonte_de_Recursos não nula
df_despesas_gold = df_despesas_gold.filter(col("Fonte_de_Recursos").isNotNull())

# Formatar a coluna 'Total_Liquidado' para exibição, sem notação científica e sem casas decimais
df_despesas_gold = df_despesas_gold.withColumn(
    "Total_Liquidado", format_number(col("Total_Liquidado"), 0)  # Sem casas decimais
)

# Exibir os resultados do df_despesas_gold
df_despesas_gold.show(truncate=False)


#Armazenando camada Gold no banco de dados
#df_despesas_gold.write.jdbc(url=url, table="despesa_gold", mode="append", properties=properties)


# Tratamento do campo 'Arrecadado_at_02_02_2024' no df_receitas_silver
df_receitas_silver = df_receitas_silver.withColumn(
    "Arrecadado_at_02_02_2024", 
    regexp_replace(col("Arrecadado_at_02_02_2024"), r'\.', '')  # Remove pontos (milhares)
)

df_receitas_silver = df_receitas_silver.withColumn(
    "Arrecadado_at_02_02_2024", 
    regexp_replace(col("Arrecadado_at_02_02_2024"), r',', '.')  # Substitui vírgula por ponto
)

# Converter para tipo inteiro (sem casas decimais)
df_receitas_silver = df_receitas_silver.withColumn(
    "Arrecadado_at_02_02_2024", col("Arrecadado_at_02_02_2024").cast("long")  # 'long' para números inteiros
)

# Agrupar por 'Fonte_de_Recursos' e somar a coluna 'Arrecadado_at_02_02_2024' no df_receitas_silver
df_receitas_gold = df_receitas_silver.groupBy("Fonte_de_Recursos").agg(
    sum("Arrecadado_at_02_02_2024").alias("Total_Arrecadado")
)

# Filtrar registros com Fonte_de_Recursos não nula
df_receitas_gold = df_receitas_gold.filter(col("Fonte_de_Recursos").isNotNull())

# Formatar a coluna 'Total_Arrecadado' para exibição, sem notação científica e sem casas decimais
df_receitas_gold = df_receitas_gold.withColumn(
    "Total_Arrecadado", format_number(col("Total_Arrecadado"), 0)  # Sem casas decimais
)

# Exibir os resultados do df_receitas_gold
df_receitas_gold.show(truncate=False)


#Armazenando camada Gold no banco de dados
#df_receitas_gold.write.jdbc(url=url, table="receita_gold", mode="append", properties=properties)



# Juntar os DataFrames df_despesas_gold e df_receitas_gold e garantir que os campos sejam combinados corretamente
df_juncao_gold = df_despesas_gold.join(
    df_receitas_gold, on="Fonte_de_Recursos", how="outer"
)

# Exibir os resultados da junção final
df_juncao_gold.show(truncate=False)

# Inserindo os dados agrupados gold no PostgreSQL
#df_juncao_gold.write.jdbc(url=url, table="orcamento", mode="append", properties=properties)


