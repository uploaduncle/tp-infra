from google.cloud import bigquery

PROJECT_ID = "usm-infra-grupo2"


def create_dataset():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.dwh"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        client.get_dataset(dataset_id)
        print("Dataset 'dwh' already exists.")
    except:
        client.create_dataset(dataset)
        print("Dataset 'dwh' created.")


def create_table(PROJECT_ID, TARGET_TABLE_ID, SQL, WRT_DISPOSITION):
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.QueryJobConfig(
        destination=TARGET_TABLE_ID,
        write_disposition=WRT_DISPOSITION
    )

    query_job = client.query(SQL, job_config=job_config)

    try:
        query_job.result()
        print(f"Query success: {TARGET_TABLE_ID}")
    except Exception as exception:
        print(f"Failed: {TARGET_TABLE_ID}")
        print(exception)


if __name__ == "__main__":
    create_dataset()

    tables = [
        {
            "name": "fact_ventas",
            "sql": f"""
                SELECT
                    codigo_cliente,
                    SKU_codigo,
                    fecha,
                    codigo_sucursal,
                    venta_unidades,
                    venta_importe,
                    condicion_venta
                FROM `{PROJECT_ID}.raw.archivos_ventaclientes`
                WHERE venta_unidades != 0 OR venta_importe != 0
                ORDER BY fecha ASC
            """
        },
        {
            "name": "fact_stock",
            "sql": f"""
                SELECT
                    codigo_sucursal,
                    SKU_codigo,
                    fecha,
                    Stock_unidades
                FROM `{PROJECT_ID}.raw.archivos_stock`
                ORDER BY fecha ASC
            """
        },
        {
            "name": "dim_cliente",
            "sql": f"""
                WITH clientes_unicos AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY codigo_cliente
                            ORDER BY fecha_alta DESC
                        ) AS rn
                    FROM `{PROJECT_ID}.raw.archivos_maestro`
                )
                SELECT
                    codigo_cliente,
                    nombre_cliente,
                    tipo_negocio,
                    provincia,
                    ciudad,
                    razon_social,
                    fecha_alta,
                    deuda_vencida,
                    estado
                FROM clientes_unicos
                WHERE rn = 1
                ORDER BY fecha ASC
            """
        },
        {
            "name": "dim_producto",
            "sql": f"""
                WITH productos_unicos AS (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY SKU_codigo
                                ORDER BY fecha DESC
                            ) AS rn
                        FROM `{PROJECT_ID}.raw.archivos_stock`
                    )
                SELECT
                    SKU_codigo,
                    SKU_descripcion,
                    unidad
                FROM productos_unicos
                WHERE rn = 1
                ORDER BY fecha ASC
            """
        },
        {
            "name": "dim_fecha",
            "sql": f"""
                SELECT DISTINCT
                    fecha,
                    EXTRACT(DAY FROM fecha) AS dia,
                    EXTRACT(MONTH FROM fecha) AS mes,
                    EXTRACT(YEAR FROM fecha) AS anio,
                    CASE FORMAT_DATE('%A', fecha)
                        WHEN 'Monday' THEN 'Lunes'
                        WHEN 'Tuesday' THEN 'Martes'
                        WHEN 'Wednesday' THEN 'Miércoles'
                        WHEN 'Thursday' THEN 'Jueves'
                        WHEN 'Friday' THEN 'Viernes'
                        WHEN 'Saturday' THEN 'Sábado'
                        WHEN 'Sunday' THEN 'Domingo'
                    END AS nombre_dia
                FROM `{PROJECT_ID}.raw.archivos_ventaclientes`
                ORDER BY fecha ASC
            """
        },
        {
            "name": "dim_sucursal",
            "sql": f"""
                WITH sucursales_unicas AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY codigo_sucursal
                        ) AS rn
                    FROM `{PROJECT_ID}.raw.archivos_maestro`
                )
                SELECT
                    codigo_sucursal,
                    direccion,
                    lat,
                    long,
                    distribuidor
                FROM sucursales_unicas
                WHERE rn = 1
                ORDER BY fecha ASC
            """
        }
    ]

    for table in tables:
        table_name = table["name"]
        sql = table["sql"]
        target_table_id = f"{PROJECT_ID}.dwh.{table_name}"
        create_table(PROJECT_ID, target_table_id, sql,
                     bigquery.WriteDisposition.WRITE_TRUNCATE)
