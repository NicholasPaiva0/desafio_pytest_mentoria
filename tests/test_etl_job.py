import pytest
from unittest.mock import Mock
from etl.etl_job import transform_to_entities


@pytest.fixture
def mock_dataframe():
    """Cria um DataFrame mockado com colunas semelhantes ao input real."""
    mock_df = Mock()
    mock_df.columns = [
        "LEI",
        "Entity.LegalName",
        "Registration.InitialRegistrationDate",
        "Entity.EntityStatus",
        "Registration.LastUpdateDate",
    ]
    return mock_df


def test_transform_to_entities_calls_selectExpr_and_select(mock_dataframe):
    """
    Testa se transform_to_entities:
    1. Chama selectExpr para renomear colunas com '.'
    2. Chama select para filtrar apenas colunas de interesse
    """
    # Arrange
    mock_snake_case_df = Mock()
    mock_snake_case_df.columns = [
        "LEI",
        "Entity_LegalName",
        "Registration_InitialRegistrationDate",
        "Entity_EntityStatus",
        "Registration_LastUpdateDate",
    ]
    mock_dataframe.selectExpr.return_value = mock_snake_case_df

    mock_final_df = Mock()
    mock_snake_case_df.select.return_value = mock_final_df

    # Act
    result = transform_to_entities(mock_dataframe)

    # Assert
    mock_dataframe.selectExpr.assert_called_once()
    mock_snake_case_df.select.assert_called_once()
    assert result == mock_final_df


def test_transform_to_entities_column_filtering(mock_dataframe):
    """
    Testa se as colunas filtradas são as esperadas.
    """
    # Arrange
    mock_snake_case_df = Mock()
    mock_snake_case_df.columns = [
        "LEI",
        "Entity_LegalName",
        "Registration_InitialRegistrationDate",
        "Entity_EntityStatus",
        "OtherColumn",
    ]
    mock_dataframe.selectExpr.return_value = mock_snake_case_df

    mock_final_df = Mock()
    mock_snake_case_df.select.return_value = mock_final_df

    # Act
    transform_to_entities(mock_dataframe)

    # Assert
    args, _ = mock_snake_case_df.select.call_args
    assert "LEI" in args
    assert "Entity_LegalName" in args
    assert "Registration_InitialRegistrationDate" in args
    assert "Entity_EntityStatus" in args
    assert "OtherColumn" not in args  # deve ser filtrada fora
