import pytest
from pyspark.sql import SparkSession
from etl.etl_job import transform_to_entities


# ---------- FIXTURE ----------
@pytest.fixture(scope="session")
def spark():
    """Cria uma sessão Spark local para todos os testes."""
    spark = (
        SparkSession.builder
        .appName("PytestETLTest")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ---------- TESTE UNITÁRIO ----------
def test_transform_to_entities(spark):
    """Valida se a função transforma corretamente as colunas."""
    # Arrange
    data = [
        ("1234", "Empresa X", "2020-01-01", "ACTIVE", "2025-01-01"),
        ("5678", "Empresa Y", "2021-01-01", "INACTIVE", "2025-01-01"),
    ]
    columns = [
        "LEI",
        "Entity.LegalName",
        "Registration.InitialRegistrationDate",
        "Entity.EntityStatus",
        "Registration.LastUpdateDate",
    ]

    df = spark.createDataFrame(data, columns)

    # Act
    df_entities = transform_to_entities(df)

    # Assert
    assert "LEI" in df_entities.columns
    assert "Entity_LegalName" in df_entities.columns
    assert "Registration_InitialRegistrationDate" in df_entities.columns
    assert df_entities.count() == 2
