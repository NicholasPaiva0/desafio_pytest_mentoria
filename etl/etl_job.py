from pyspark.sql import SparkSession, DataFrame

def create_spark() -> SparkSession:
    """Cria e retorna uma SparkSession (em Glue seria via GlueContext)."""
    spark = (
        SparkSession.builder
        .appName("GlueETLJob")
        .getOrCreate()
    )
    return spark


def read_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Lê arquivo CSV do S3 e retorna DataFrame."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def write_raw_data(df: DataFrame, path: str) -> None:
    """Escreve DataFrame no formato CSV (camada raw)."""
    df.write.mode("overwrite").option("header", "true").csv(path)


def transform_to_entities(df: DataFrame) -> DataFrame:
    """Seleciona colunas relevantes para camada curated (entities)."""
    df_snake_case = df.selectExpr([
        f"`{col}` as `{col.replace('.', '_')}`" for col in df.columns
    ])

    entity_cols = [col for col in df_snake_case.columns if col.startswith("Entity") 
                   or col.startswith("LEI") 
                   or col.startswith("Registration_InitialRegistrationDate") 
                   or col.startswith("Registration_LastUpdateDate")]

    return df_snake_case.select(*entity_cols)


def write_curated_entities(df: DataFrame, path: str) -> None:
    """Salva camada curated particionada."""
    df.write.mode("overwrite").partitionBy("Entity_LegalAddress_Country").parquet(path)


def create_specialized_layer(spark: SparkSession, df: DataFrame, path: str) -> None:
    """Cria camada specialized a partir da curated."""
    df.createOrReplaceTempView("entity_temp")

    result = spark.sql("""
        SELECT cast(LEI as string) as lei,
               cast(Entity_LegalName as string) as legal_name,
               cast(Registration_InitialRegistrationDate as string) as registration_date,
               CASE WHEN Entity_EntityStatus in ("ACTIVE") then "Sim" else "Não" end as is_active,
               cast(current_date as string) as snapshot_date
        FROM entity_temp
        WHERE lei IS NOT NULL
    """)
    result.write.mode("overwrite").parquet(path)


def run_etl_job(raw_input: str, raw_output: str, curated_output: str, specialized_output: str):
    """Orquestra o pipeline ETL."""
    spark = create_spark()

    df_raw = read_raw_data(spark, raw_input)
    write_raw_data(df_raw, raw_output)

    df_entities = transform_to_entities(df_raw)
    write_curated_entities(df_entities, curated_output)

    create_specialized_layer(spark, df_entities, specialized_output)

    spark.stop()
