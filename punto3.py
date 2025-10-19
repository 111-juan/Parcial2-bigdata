import boto3
import pandas as pd
import holidays
from io import BytesIO

# --- Configuración ---
s3_input_bucket = "s3-parcial2"
s3_input_prefix = "fact_rental/"   # Carpeta donde están los archivos fuente
s3_output_prefix = "dim_date/"     # Carpeta donde guardaremos la tabla dim_date
aws_region = "us-east-1"

# Inicializa cliente S3
s3 = boto3.client("s3", region_name=aws_region)

# --- 1. Listar archivos Parquet en S3 ---
response = s3.list_objects_v2(Bucket=s3_input_bucket, Prefix=s3_input_prefix)
parquet_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]

if not parquet_files:
    raise Exception("❌ No se encontraron archivos Parquet en la ruta fact_rental/")

print(f"📂 Archivos encontrados: {len(parquet_files)}")

# --- 2. Leer los archivos Parquet ---
dataframes = []
for file in parquet_files:
    print(f"🔹 Leyendo archivo: {file}")
    obj = s3.get_object(Bucket=s3_input_bucket, Key=file)
    df = pd.read_parquet(BytesIO(obj["Body"].read()), engine="fastparquet")

    # 🔧 Validar columna rental_date
    if "rental_date" not in df.columns:
        raise Exception(f"❌ El archivo {file} no contiene la columna 'rental_date'")

    dataframes.append(df)

# Combinar todos los archivos
df_all = pd.concat(dataframes, ignore_index=True)
print(f"✅ Total de registros cargados: {len(df_all)}")

# --- 3. Validar tipo de dato de rental_date ---
if not pd.api.types.is_datetime64_any_dtype(df_all["rental_date"]):
    df_all["rental_date"] = pd.to_datetime(df_all["rental_date"], errors="coerce")

# Eliminar registros sin fecha
df_all = df_all.dropna(subset=["rental_date"])

# Convertir a tipo date (sin hora)
df_all["rental_date"] = pd.to_datetime(df_all["rental_date"]).dt.date

# --- 4. Crear tabla de dimensión de fechas ---
df_dim = df_all[["rental_date"]].drop_duplicates().copy()

# Crear campos derivados
df_dim["date_id"] = df_dim["rental_date"].apply(lambda x: int(x.strftime("%Y%m%d")))
df_dim["day_of_week"] = pd.to_datetime(df_dim["rental_date"]).dt.day_name()
df_dim["is_weekend"] = df_dim["day_of_week"].isin(["Saturday", "Sunday"])
df_dim["quarter"] = pd.to_datetime(df_dim["rental_date"]).dt.quarter

# --- 5. Agregar festivos de EE.UU. ---
us_holidays = holidays.US()
df_dim["is_holiday"] = df_dim["rental_date"].apply(lambda x: x in us_holidays)

# --- 6. Reordenar columnas ---
df_dim = df_dim[["date_id", "rental_date", "is_weekend", "is_holiday", "day_of_week", "quarter"]]

# --- 7. Guardar resultado en S3 en formato Parquet (Snappy) ---
output_buffer = BytesIO()
df_dim.to_parquet(output_buffer, index=False, compression="snappy", engine="fastparquet")

s3.put_object(
    Bucket=s3_input_bucket,
    Key=f"{s3_output_prefix}dim_date.snappy.parquet",
    Body=output_buffer.getvalue()
)

print("✅ ETL completado exitosamente. Archivo 'dim_date.snappy.parquet' guardado en s3://s3-parcial2/dim_date/")
