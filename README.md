# Desafio Pytest Mentoria

Este repositório contém um estudo prático de **modularização de código ETL em Python com PySpark** e a criação de **testes unitários usando Pytest**.  
O objetivo é praticar **boas práticas de testes**, incluindo o uso de **mocks** para simular objetos Spark e deixar os testes mais rápidos e independentes.

---

## 📂 Estrutura do projeto

desafio_pytest_mentoria/
├── etl/
│ ├── init.py
│ ├── etl_job.py # Código ETL modularizado
│
├── tests/
│ ├── init.py
│ ├── test_etl_job.py # Testes unitários usando Mock
│
├── README.md
├── requirements.txt

---

## ⚙️ Tecnologias

- [Python 3.10+](https://www.python.org/)
- [PySpark](https://spark.apache.org/docs/latest/api/python/) (simulado via Mock)
- [Pytest](https://docs.pytest.org/en/stable/)
- [unittest.mock](https://docs.python.org/3/library/unittest.mock.html) (para simulação de objetos)

---

## 🚀 Como rodar o projeto

### 1. Clonar o repositório
```bash
git clone https://github.com/NicholasPaiva0/desafio_pytest_mentoria.git
cd desafio_pytest_mentoria
