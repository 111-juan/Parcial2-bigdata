import pandas as pd
import pytest
from datetime import date

@pytest.fixture
def sample_data():
    """Simula un DataFrame de salida del ETL."""
    data = {
        "rental_date": [date(2005, 5, 24), date(2005, 5, 25)],
        "date_id": [20050524, 20050525],
        "day": [24, 25],
        "month": [5, 5],
        "year": [2005, 2005],
        "day_of_week": ["Tuesday", "Wednesday"],
        "week_of_year": [21, 21],
    }
    return pd.DataFrame(data)


def test_fecha_formato(sample_data):
    """1️⃣ Validar que rental_date tenga formato de fecha (YYYY-MM-DD)."""
    for value in sample_data["rental_date"]:
        assert isinstance(value, date), f"❌ rental_date no es tipo fecha: {value}"
        assert value.strftime("%Y-%m-%d") == str(value), f"❌ Formato incorrecto: {value}"


def test_date_id_formato(sample_data):
    """2️⃣ Validar que date_id esté en formato YYYYMMDD."""
    for date_val, date_id in zip(sample_data["rental_date"], sample_data["date_id"]):
        esperado = int(date_val.strftime("%Y%m%d"))
        assert date_id == esperado, f"❌ date_id incorrecto ({date_id}) != {esperado}"


def test_columnas_correctas(sample_data):
    """3️⃣ Validar que solo existan las columnas esperadas."""
    columnas_esperadas = ["rental_date", "date_id", "day", "month", "year", "day_of_week", "week_of_year"]
    columnas_encontradas = list(sample_data.columns)
    assert columnas_encontradas == columnas_esperadas, f"❌ Columnas inesperadas: {columnas_encontradas}"
 