# Usa uma imagem base do Airflow com Python
FROM apache/airflow:2.5.0-python3.9

# Instala dependências necessárias, incluindo o cliente PostgreSQL e o Hadoop
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk wget libpq-dev gcc curl

# Define o JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Instala o driver do PostgreSQL
USER airflow
RUN pip install psycopg2-binary

# Instalar o provedor do Apache Spark
RUN pip install apache-airflow-providers-apache-spark

# Instalar o PySpark
RUN pip install pyspark==3.4.4

# Baixa e configura o Apache Spark
USER root
RUN wget https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.4-bin-hadoop3.tgz && \
    mv spark-3.4.4-bin-hadoop3 /usr/local/spark && \
    rm spark-3.4.4-bin-hadoop3.tgz

# Baixar e instalar o Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xzf hadoop-3.4.1.tar.gz && \
    mv hadoop-3.4.1 /usr/local/hadoop && \
    rm hadoop-3.4.1.tar.gz



# Instalar procps (necessário para o Spark)
RUN apt-get update && apt-get install -y procps

# Define as variáveis de ambiente do Spark e Hadoop
ENV SPARK_HOME /usr/local/spark
ENV HADOOP_HOME /usr/local/hadoop
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV PYSPARK_PYTHON /usr/local/bin/python
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop

# Configura o Spark para rodar em modo local
RUN echo "spark.master                    spark://localhost:7077" > $SPARK_HOME/conf/spark-defaults.conf


# Define o usuário do Airflow
USER airflow


# Define o ponto de entrada padrão
ENTRYPOINT ["airflow", "standalone"]
