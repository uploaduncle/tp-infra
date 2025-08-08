# TP Final - Infraestructura para Ciencia de Datos

**Universidad Nacional de San Martín (UNSAM)**
**Licenciatura en Ciencia de Datos**

*Integrantes:* Lucas Golchtein, Marcos Achaval, Ludmila Cáceres, Iván Vergara

---

## Descripción del Proyecto

Este proyecto implementa una **arquitectura completa de datos** que incluye:

- **Generación de datos sintéticos** de ventas para distribuidoras
- **Pipeline ETL** para procesar datos desde Storage hasta Data Marts
- **Data Warehouse** con esquema estrella
- **Data Marts especializados** por área de negocio

## Arquitectura

```
📁 Datos Raw (Google Cloud Storage)
    ↓
📊 Dataset RAW (BigQuery)
    ↓
🏢 Data Warehouse (BigQuery)
    ↓
📈 Data Marts (BigQuery)
    ├── Marketing
    ├── Logística  
    └── Finanzas
```

## Estructura del Proyecto

```
tp-infra/
├── 📁 src/                          # Código fuente
│   ├── crear_raw.py                 # Carga datos RAW
│   ├── crear_data_warehouse.py      # Construye DWH
│   ├── crear_data_marts.py          # Construye Data Marts
│   └── generador_de_archivos.ipynb  # Generación de datos
├── 📁 config/                       # Configuración
│   └── config.py                    # Variables centralizadas
├── 📁 docs/                         # Documentación
│   ├── der_estrella_dwh.mermaid     # Diagrama DER
│   ├── diagrama_flujo_informacion.drawio # Diagrama de flujo
│   ├── especificación.xlsx          # Especificaciones del proyecto
│   └── presentación_final.pdf       # Presentación del TP
├── .env.example                     # Plantilla de configuración
├── .gitignore                       # Archivos ignorados
├── requirements.txt                 # Dependencias Python
└── README.md                        # Este archivo
```

## Configuración

### 1. Prerrequisitos

- **Python 3.8+**
- **Cuenta de Google Cloud Platform**
- **Proyecto en GCP con BigQuery habilitado**
- **Bucket en Google Cloud Storage**

### 2. Instalación

```bash
# Clonar repositorio
git clone https://github.com/MarcosACH/tp-infra.git
cd tp-infra

# Instalar dependencias
pip install -r requirements.txt

# Configurar credenciales
cp .env.example .env
# Editar .env con tus credenciales reales
```

### 3. Configurar Credenciales

1. **Crear archivo `.env`** desde la plantilla:

   ```bash
   cp .env.example .env
   ```
2. **Descargar credenciales de GCP**:

   - Ve a Google Cloud Console
   - IAM & Admin → Service Accounts
   - Crea/descarga archivo JSON de credenciales
3. **Completar `.env`**:

   ```bash
   GCP_PROJECT_ID=tu-proyecto-gcp
   GCP_BUCKET_NAME=tu-bucket-datos
   GOOGLE_APPLICATION_CREDENTIALS=./config/tu-archivo-credenciales.json
   ```

## Uso

### Generar Datos Sintéticos

1. Abrir `src/generador_de_archivos.ipynb`
2. Configurar parámetros (días, distribuidores, clientes)
3. Ejecutar todas las celdas
4. Los datos se suben automáticamente a Google Cloud Storage

### Ejecutar Pipeline Completo

```bash
cd src
python crear_raw.py              # 1. Cargar datos RAW
python crear_data_warehouse.py   # 2. Construir DWH  
python crear_data_marts.py       # 3. Construir Data Marts
```

## Datasets Generados

### RAW Layer

- `archivos_maestro` - Información de clientes y sucursales
- `archivos_stock` - Inventario por producto y fecha
- `archivos_ventaclientes` - Transacciones de ventas

### Data Warehouse

- **Dimensiones**: `dim_cliente`, `dim_producto`, `dim_fecha`, `dim_sucursal`
- **Hechos**: `fact_ventas`, `fact_stock`

### Data Marts

- **Marketing**: Análisis de ventas por cliente, producto y región
- **Logística**: Optimización de rutas y distribución geográfica
- **Finanzas**: Control de ingresos, costos y deudas

## Tecnologías Utilizadas

- **Python 3.8+** - Lenguaje principal
- **Google Cloud BigQuery** - Data Warehouse
- **Google Cloud Storage** - Almacenamiento de archivos (Bucket)
- **Pandas** - Manipulación de datos
- **Faker** - Generación de datos sintéticos
- **Jupyter Notebooks** - Análisis exploratorio

## Equipo

**Grupo 2 - Infraestructura para Ciencia de Datos**
Universidad Nacional de San Martín (UNSAM)
*Integrantes:* Lucas Golchtein, Marcos Achaval, Ludmila Cáceres, Iván Vergara

## Licencia

Este proyecto es parte del trabajo práctico final de la materia Infraestructura para Ciencia de Datos.

---

## Ayuda y Solución de Problemas

### Error de Credenciales

```bash
# Verificar que el archivo .env existe y tiene las variables correctas
cat .env
```

### Error de Permisos en GCP

- Verificar que el Service Account tiene permisos de BigQuery Admin
- Verificar que el Service Account tiene permisos de Storage Admin

### Error de Dependencias

```bash
# Reinstalar dependencias
pip install -r requirements.txt --upgrade
```
