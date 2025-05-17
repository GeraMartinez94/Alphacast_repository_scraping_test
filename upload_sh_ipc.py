import os
from dotenv import load_dotenv
from sqlite3 import Date
from typing import Optional
from alphacast import Alphacast
import pandas as pd
from alphacast import Alphacast

API_KEY = os.getenv("ALPHACAST_API_KEY")
alphacast_client = Alphacast(API_KEY)

DATASET_NAME = "Precios Promedio Anuales - Script"
DATASET_DESCRIPTION = "Dataset creado automáticamente por el script"
DATASET_COLUMNS_DEFINITION = [
    {"name": "date", "type": "YEAR", "isDate": True},
    {"name": "Región", "type": "STRING", "isEntity": True},
    {"name": "Productos Seleccionados", "type": "STRING", "isEntity": True},
    {"name": "Unidad De Medida", "type": "STRING", "isEntity": True},
    {"name": "Valor", "type": "NUMBER"}
]

REPO_ID = os.getenv("ALPHACAST_REPO_ID")

#####################################
# Funcion que lee el archivo XLSX
#####################################
def leer_excel(archivo_xlsx):
    try:
        df = pd.read_excel(archivo_xlsx, header=[1, 2])
        return df
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo XLSX en la ruta: {archivo_xlsx}")
        return None
    except Exception as e:
        print(f"Ocurrió un error al leer el archivo XLSX: {e}")
        return None

#####################################
# Funcion que prepara la informacion obtenida del archivo XLSX
#####################################
def preparar_datos(df, manejo_duplicados='eliminar'):
    if df is None:
        return None
    df = df.dropna(subset=[('Unnamed: 0_level_0', 'Región')], how='all')
    df = df.dropna(subset=[('Unnamed: 1_level_0', 'Productos seleccionados')], how='all')
    df = df.dropna(subset=[('Unnamed: 2_level_0', 'Unidad de medida')], how='all')
    nuevas_columnas = []
    for col in df.columns:
        if isinstance(col, tuple):
            if 'Año' in col[1]:
                año = col[1].split(' ')[-1]
                nuevas_columnas.append(año)
            elif col[1] in ['Región', 'Productos seleccionados', 'Unidad de medida']:
                nuevas_columnas.append(col[1])
            else:
                nuevas_columnas.append(col[1])
        else:
            nuevas_columnas.append(col)
    df.columns = nuevas_columnas
    id_vars = ['Región', 'Productos seleccionados', 'Unidad de medida']
    year_cols = [col for col in df.columns if col not in id_vars]
    df_melted = pd.melt(df, id_vars=id_vars, value_vars=year_cols, var_name='Anio', value_name='Valor')

    print("Columnas en df_melted antes del rename:", df_melted.columns)
    for col in ['Región', 'Productos seleccionados', 'Unidad de medida']:
        if col in df_melted.columns:
            print(f"Renombrando '{col}' a '{col.title()}'")
            df_melted.rename(columns={col: col.title()}, inplace=True)
    print("Columnas en df_melted después del rename:", df_melted.columns)

    if 'Anio' in df_melted.columns:
        df_melted['date'] = pd.to_datetime(df_melted['Anio'], format='%Y').dt.year
    df_melted['Valor'] = pd.to_numeric(df_melted['Valor'], errors='coerce')
    df_melted = df_melted.dropna(subset=['Valor', 'date'])

    columnas_para_duplicados = ['date', 'Región', 'Productos Seleccionados', 'Unidad De Medida', 'Valor']
    print("Columnas para duplicados:", columnas_para_duplicados)

    if manejo_duplicados == 'eliminar':
        df_melted_agg = df_melted.drop_duplicates(subset=columnas_para_duplicados, keep='first')
    elif manejo_duplicados == 'primero':
        try:
            df_melted_agg = df_melted.groupby(columnas_para_duplicados[:-1], dropna=False).agg({'Valor': 'first'}).reset_index()
        except KeyError as e:
            print(f"Error: La columna {e} no existe. Asegúrate de que las columnas para agrupar sean correctas.")
            raise
    elif manejo_duplicados == 'promedio':
        try:
            df_melted_agg = df_melted.groupby(columnas_para_duplicados[:-1], dropna=False).agg({'Valor': 'mean'}).reset_index()
        except KeyError as e:
            print(f"Error: La columna {e} no existe. Asegúrate de que las columnas para agrupar sean correctas.")
            raise
    elif manejo_duplicados == 'suma':
        try:
            df_melted_agg = df_melted.groupby(columnas_para_duplicados[:-1], dropna=False).agg({'Valor': 'sum'}).reset_index()
        except KeyError as e:
            print(f"Error: La columna {e} no existe. Asegúrate de que las columnas para agrupar sean correctas.")
            raise
    elif callable(manejo_duplicados):
        try:
            df_melted_agg = df_melted.groupby(columnas_para_duplicados[:-1], dropna=False).agg({'Valor': manejo_duplicados}).reset_index()
        except KeyError as e:
            print(f"Error: La columna {e} no existe. Asegúrate de que las columnas para agrupar sean correctas.")
            raise
    else:
        print("Opción de manejo de duplicados no válida. Se eliminarán los duplicados.")
        df_melted_agg = df_melted.drop_duplicates(subset=columnas_para_duplicados, keep='first')
    return df_melted_agg

#####################################
# Funcion que muestra los datos del XLSX
#####################################
def mostrar_datos(df, num_filas=5, mostrar_info=True):
    if df is None:
        print("No hay datos para mostrar.")
        return
    print("\nPrimeras filas del DataFrame:")
    print(df.head(num_filas))
    if mostrar_info:
        print("\nInformación del DataFrame:")
        df.info()

#####################################
# Funcion que sube la informacion a Alphacast
#####################################
def subir_datos_a_alphacast_v2(
    df: pd.DataFrame,
    dataset_id: int,
    alphacast_client: Alphacast,
    date_column: str = "date",
    value_column: str = "Valor",
    entity_columns: Optional[list[str]] = None,
    date_format: Optional[str] = None,
    initialize: bool = False,
    delete_missing: bool = False,
    on_conflict_update: bool = False,
    upload_index: bool = False,
):
    if date_column not in df.columns:
        raise ValueError(f"DataFrame debe tener la columna de fecha: '{date_column}'")
    if value_column not in df.columns:
        raise ValueError(f"DataFrame debe tener la columna de valor: '{value_column}'")
    if entity_columns:
        for col in entity_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame debe tener la columna de entidad: '{col}'")
    try:
        alphacast_dataset = alphacast_client.datasets.dataset(dataset_id)
        df_to_upload = df.copy()
        if value_column != "value":
            df_to_upload = df_to_upload.rename(columns={value_column: "value"})
        alphacast_dataset.upload_data_from_df(
            df_to_upload,
            deleteMissingFromDB=delete_missing,
            onConflictUpdateDB=on_conflict_update,
            uploadIndex=upload_index
        )
        print(f"✅ Datos subidos correctamente al dataset {dataset_id}.")
    except Exception as e:
        print(f"❌ Error al subir datos al dataset {dataset_id}: {e}")
        print(f"  Detalles del error: {e}")

# Ejemplo de uso:
if __name__ == "__main__":
    XLSX_FILE_PATH = os.getenv("XLS_FILE_PATH")

    print(f"Creando dataset '{DATASET_NAME}'...")
    dataset_definition = {
        "DataSet": DATASET_NAME,
        "description": DATASET_DESCRIPTION,
        "columns": DATASET_COLUMNS_DEFINITION
    }
    dataset_id = None
    try:
        new_dataset = alphacast_client.datasets.create(dataset_definition, repo_id=REPO_ID)
        print("Dataset creado exitosamente:", new_dataset)
        dataset_id = new_dataset.get("id")
        if dataset_id is None:
            print("Error al obtener el ID del nuevo dataset.")
            exit()
    except Exception as e:
        print(f"Error al crear el dataset desde la API: {e}")
        exit()

    df = leer_excel(XLSX_FILE_PATH)

    if df is not None:
        df_preparado = preparar_datos(df.copy(), manejo_duplicados='eliminar')
        print(f"Number of rows after preparing data: {len(df_preparado)}")
        print("First 10 rows after preparing data:")
        print(df_preparado.head(10))
        mostrar_datos(df_preparado)

        fecha_columna_proyecto = 'date'
        valor_columna_proyecto = 'Valor'
        entidades_columnas_proyecto = ['Región', 'Productos Seleccionados', 'Unidad De Medida']

        try:
            subir_datos_a_alphacast_v2(
                df=df_preparado.copy(),
                dataset_id=dataset_id,
                alphacast_client=alphacast_client,
                date_column=fecha_columna_proyecto,
                value_column=valor_columna_proyecto,
                entity_columns=entidades_columnas_proyecto,
                date_format='%Y',
                initialize=False,
                delete_missing=False,
                on_conflict_update=True,
            )
        except ValueError as ve:
            print(f"Error de formato del DataFrame: {ve}")
        except Exception as e:
            print(f"Error general durante la subida: {e}")