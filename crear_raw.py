from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
import pandas as pd
import io
import re

# ------------------- CREAR DATASET -------------------
PROJECT_ID = "usm-infra-grupo2"
client = bigquery.Client(project=PROJECT_ID)

dataset_name = 'raw'
location = 'US'


def create_bigquery_dataset(dataset_name):
    dataset_id = f"{client.project}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {client.project}.{dataset.dataset_id}")


create_bigquery_dataset(dataset_name)

# ------------------- FUNCIONES COMUNES -------------------


def extract_date_from_filename(filename):
    match = re.search(r'(\d{4}[-_]?\d{2}[-_]?\d{2})', filename)
    return match.group(1) if match else 'unknown'

# ------------------- CARGAR MAESTRO -------------------


def cargar_maestro():
    BUCKET_NAME = "bucket_grupo2_infra"
    SOURCE_FOLDER = "generated_data/Archivos_Maestro/"
    OUTPUT_BLOB = "processed_data/maestro.csv"
    TABLE_ID = f"{PROJECT_ID}.raw.archivos_maestro"

    maestro_schema = [
        bigquery.SchemaField("codigo_sucursal", "INT64"),
        bigquery.SchemaField("codigo_cliente", "INT64"),
        bigquery.SchemaField("ciudad", "STRING"),
        bigquery.SchemaField("provincia", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("nombre_cliente", "STRING"),
        bigquery.SchemaField("cuit", "INT64"),
        bigquery.SchemaField("razon_social", "STRING"),
        bigquery.SchemaField("direccion", "STRING"),
        bigquery.SchemaField("dias_visita", "STRING"),
        bigquery.SchemaField("telefono", "STRING"),
        bigquery.SchemaField("fecha_alta", "DATE"),
        bigquery.SchemaField("fecha_baja", "STRING"),
        bigquery.SchemaField("lat", "FLOAT64"),
        bigquery.SchemaField("long", "FLOAT64"),
        bigquery.SchemaField("condicion_venta", "STRING"),
        bigquery.SchemaField("deuda_vencida", "FLOAT64"),
        bigquery.SchemaField("tipo_negocio", "STRING"),
        bigquery.SchemaField("distribuidor", "STRING"),
        bigquery.SchemaField("fecha", "DATE"),
    ]

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=SOURCE_FOLDER)

    all_data = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            match = re.search(r'Distribuidor_\d+', blob.name)
            distribuidor = match.group(0) if match else 'Desconocido'
            date = extract_date_from_filename(blob.name)
            content = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(content))
            df["distribuidor"] = distribuidor
            df["fecha"] = date
            all_data.append(df)

    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df.to_csv("maestro_temp.csv", index=False)
    bucket.blob(OUTPUT_BLOB).upload_from_filename("maestro_temp.csv")
    print(f"Uploaded merged file to: gs://{BUCKET_NAME}/{OUTPUT_BLOB}")

    job_config = bigquery.LoadJobConfig(
        schema=maestro_schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )
    uri = f"gs://{BUCKET_NAME}/{OUTPUT_BLOB}"
    load_job = client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"Loaded data into {TABLE_ID}")

# ------------------- CARGAR STOCK -------------------


def cargar_stock():
    BUCKET_NAME = "bucket_grupo2_infra"
    SOURCE_FOLDER = "generated_data/Archivos_Stock/"
    OUTPUT_BLOB = "processed_data/stock.csv"
    TABLE_ID = f"{PROJECT_ID}.raw.archivos_stock"

    stock_schema = [
        bigquery.SchemaField("codigo_sucursal", "INT64"),
        bigquery.SchemaField("fecha_cierre_comercial", "DATE"),
        bigquery.SchemaField("SKU_codigo", "STRING"),
        bigquery.SchemaField("SKU_descripcion", "STRING"),
        bigquery.SchemaField("Stock_unidades", "INT64"),
        bigquery.SchemaField("unidad", "STRING"),
        bigquery.SchemaField("distribuidor", "STRING"),
        bigquery.SchemaField("fecha", "DATE"),
    ]

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=SOURCE_FOLDER)

    all_data = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            match = re.search(r'Distribuidor_\d+', blob.name)
            distribuidor = match.group(0) if match else 'Desconocido'
            date = extract_date_from_filename(blob.name)
            content = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(content))
            df["distribuidor"] = distribuidor
            df["fecha"] = date
            all_data.append(df)

    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df.to_csv("maestro_temp.csv", index=False)
    bucket.blob(OUTPUT_BLOB).upload_from_filename("maestro_temp.csv")
    print(f"Uploaded merged file to: gs://{BUCKET_NAME}/{OUTPUT_BLOB}")

    job_config = bigquery.LoadJobConfig(
        schema=stock_schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )
    uri = f"gs://{BUCKET_NAME}/{OUTPUT_BLOB}"
    load_job = client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"Loaded data into {TABLE_ID}")

# ------------------- CARGAR VENTAS CLIENTES -------------------


def cargar_ventas_clientes():
    BUCKET_NAME = "bucket_grupo2_infra"
    SOURCE_FOLDER = "generated_data/Archivos_VentaClientes/"
    OUTPUT_BLOB = "processed_data/ventas.csv"
    TABLE_ID = f"{PROJECT_ID}.raw.archivos_ventaclientes"

    clientes_schema = [
        bigquery.SchemaField("codigo_sucursal", "INT64"),
        bigquery.SchemaField("codigo_cliente", "INT64"),
        bigquery.SchemaField("fecha_cierre_comercial", "DATE"),
        bigquery.SchemaField("SKU_codigo", "STRING"),
        bigquery.SchemaField("venta_unidades", "INT64"),
        bigquery.SchemaField("venta_importe", "FLOAT64"),
        bigquery.SchemaField("condicion_venta", "STRING"),
        bigquery.SchemaField("distribuidor", "STRING"),
        bigquery.SchemaField("fecha", "DATE"),
    ]

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=SOURCE_FOLDER)

    all_data = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            match = re.search(r'Distribuidor_\d+', blob.name)
            distribuidor = match.group(0) if match else 'Desconocido'
            date = extract_date_from_filename(blob.name)
            content = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(content))
            df["distribuidor"] = distribuidor
            df["fecha"] = date
            all_data.append(df)

    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df.to_csv("maestro_temp.csv", index=False)
    bucket.blob(OUTPUT_BLOB).upload_from_filename("maestro_temp.csv")
    print(f"Uploaded merged file to: gs://{BUCKET_NAME}/{OUTPUT_BLOB}")

    job_config = bigquery.LoadJobConfig(
        schema=clientes_schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )
    uri = f"gs://{BUCKET_NAME}/{OUTPUT_BLOB}"
    load_job = client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"Loaded data into {TABLE_ID}")

# ------------------- MAIN -------------------


if __name__ == "__main__":
    cargar_maestro()
    cargar_stock()
    cargar_ventas_clientes()
