from google.cloud import bigquery

PROJECT_ID = "usm-infra-grupo2"


def create_dataset():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.dms"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    try:
        client.get_dataset(dataset_id)
        print("Dataset 'dms' already exists.")
    except:
        client.create_dataset(dataset)
        print("Dataset 'dms' created.")


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
            "name": "dm_marketing",
            "sql": f"""
                SELECT
                    v.codigo_cliente AS codigo_cliente,
                    p.SKU_codigo AS SKU_codigo,
                    p.SKU_descripcion AS SKU_descripcion,
                    v.fecha AS fecha,
                    CASE FORMAT_DATE('%A', v.fecha)
                        WHEN 'Monday' THEN 'Lunes'
                        WHEN 'Tuesday' THEN 'Martes'
                        WHEN 'Wednesday' THEN 'Miércoles'
                        WHEN 'Thursday' THEN 'Jueves'
                        WHEN 'Friday' THEN 'Viernes'
                        WHEN 'Saturday' THEN 'Sábado'
                        WHEN 'Sunday' THEN 'Domingo'
                    END AS nombre_dia,
                    CASE FORMAT_DATE('%A', v.fecha)
                        WHEN 'Monday' THEN 1
                        WHEN 'Tuesday' THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday' THEN 4
                        WHEN 'Friday' THEN 5
                        WHEN 'Saturday' THEN 6
                        WHEN 'Sunday' THEN 7
                    END AS numero_dia_semana,
                    CASE
                    WHEN (EXTRACT(MONTH FROM v.fecha)) = 5 THEN "Mayo" 
                    ELSE "Junio" 
                    END AS nombre_mes,
                    p.unidad AS unidad,
                    c.tipo_negocio AS tipo_negocio,
                    s.distribuidor AS distribuidor,
                    c.provincia AS provincia,
                    c.ciudad AS ciudad,
                    v.venta_unidades AS venta_unidades,
                    v.venta_importe AS venta_importe,
                    v.condicion_venta AS condicion_venta,
                    c.estado AS estado,
                FROM `{PROJECT_ID}.dwh.fact_ventas` v
                LEFT JOIN `{PROJECT_ID}.dwh.dim_cliente` c ON c.codigo_cliente = v.codigo_cliente
                LEFT JOIN `{PROJECT_ID}.dwh.dim_producto` p ON p.SKU_codigo = v.SKU_codigo
                LEFT JOIN `{PROJECT_ID}.dwh.dim_sucursal` s ON s.codigo_sucursal = v.codigo_sucursal
            """
        },
        {
            "name": "dm_logistica",
            "sql": f"""
                SELECT
                    s.codigo_sucursal AS codigo_sucursal,
                    v.fecha AS fecha,
                    CASE FORMAT_DATE('%A', v.fecha)
                        WHEN 'Monday' THEN 'Lunes'
                        WHEN 'Tuesday' THEN 'Martes'
                        WHEN 'Wednesday' THEN 'Miércoles'
                        WHEN 'Thursday' THEN 'Jueves'
                        WHEN 'Friday' THEN 'Viernes'
                        WHEN 'Saturday' THEN 'Sábado'
                        WHEN 'Sunday' THEN 'Domingo'
                    END AS nombre_dia,
                    CASE
                    WHEN (EXTRACT(MONTH FROM v.fecha)) = 5 THEN "Mayo" 
                    ELSE "Junio" 
                    END AS nombre_mes,
                    c.provincia AS provincia,
                    c.ciudad AS ciudad,
                    c.razon_social AS razon_social,
                    s.direccion AS direccion,
                    s.distribuidor AS distribuidor,
                    s.long AS long,
                    s.lat AS lat,
                    c.tipo_negocio AS tipo_negocio,
                    v.venta_unidades AS venta_unidades,
                FROM `{PROJECT_ID}.dwh.fact_ventas` v
                LEFT JOIN `{PROJECT_ID}.dwh.dim_cliente` c ON c.codigo_cliente = v.codigo_cliente
                LEFT JOIN `{PROJECT_ID}.dwh.dim_sucursal` s ON s.codigo_sucursal = v.codigo_sucursal
            """
        },
        {
            "name": "dm_finanzas",
            "sql": f"""
                SELECT
                    v.codigo_sucursal AS codigo_sucursal,
                    p.SKU_codigo AS SKU_codigo,
                    v.fecha AS fecha,
                    CASE FORMAT_DATE('%A', v.fecha)
                        WHEN 'Monday' THEN 'Lunes'
                        WHEN 'Tuesday' THEN 'Martes'
                        WHEN 'Wednesday' THEN 'Miércoles'
                        WHEN 'Thursday' THEN 'Jueves'
                        WHEN 'Friday' THEN 'Viernes'
                        WHEN 'Saturday' THEN 'Sábado'
                        WHEN 'Sunday' THEN 'Domingo'
                    END AS nombre_dia,
                    CASE FORMAT_DATE('%A', v.fecha)
                        WHEN 'Monday' THEN 1
                        WHEN 'Tuesday' THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday' THEN 4
                        WHEN 'Friday' THEN 5
                        WHEN 'Saturday' THEN 6
                        WHEN 'Sunday' THEN 7
                    END AS numero_dia_semana,
                    CASE
                    WHEN (EXTRACT(MONTH FROM v.fecha)) = 5 THEN "Mayo" 
                    ELSE "Junio" 
                    END AS nombre_mes,
                    c.tipo_negocio,
                    p.SKU_descripcion AS SKU_descripcion,
                    st.Stock_unidades AS Stock_unidades,
                    s.distribuidor AS distribuidor,
                    p.unidad AS unidad,
                    v.venta_unidades AS venta_unidades,
                    v.venta_importe AS venta_importe,
                    v.condicion_venta AS condicion_venta,
                    c.deuda_vencida AS deuda_vencida,

                FROM `{PROJECT_ID}.dwh.fact_ventas` v
                LEFT JOIN `{PROJECT_ID}.dwh.dim_cliente` c ON c.codigo_cliente = v.codigo_cliente
                LEFT JOIN `{PROJECT_ID}.dwh.dim_sucursal` s ON s.codigo_sucursal = v.codigo_sucursal
                LEFT JOIN `{PROJECT_ID}.dwh.fact_stock` st 
                ON st.codigo_sucursal = v.codigo_sucursal
                AND st.SKU_codigo = v.SKU_codigo
                AND st.fecha = v.fecha
                LEFT JOIN `{PROJECT_ID}.dwh.dim_producto` p ON p.SKU_codigo = v.SKU_codigo
            """
        }
    ]

    for table in tables:
        table_name = table["name"]
        sql = table["sql"]
        target_table_id = f"{PROJECT_ID}.dms.{table_name}"
        create_table(PROJECT_ID, target_table_id, sql,
                     bigquery.WriteDisposition.WRITE_TRUNCATE)
