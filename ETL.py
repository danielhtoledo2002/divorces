import polars as pl 
import os 
import os
from google.cloud import storage
from pandas_gbq import to_gbq



############################ Funtion To Processs all Joins ###################################

def do_joins_to_main(lista_archivos, conjunto_de_datos):

    for archivo in lista_archivos:
        list_joins = diccionario_de_datos.filter(pl.col("catalogo") == archivo)[["nemonico", "catalogo"]]

        name_csv = list_joins.select(pl.col("catalogo").unique())["catalogo"].to_list()
        name_column_real_dataset = list_joins.select(pl.col("nemonico"))["nemonico"].to_list()

        for column in name_column_real_dataset:
            if len(name_csv) < 1 :
                print("not working")
                continue
            else:

                blob = bucket.blob(f"catalogos/{name_csv[0]}.csv")

                with blob.open("r") as f:
                    file_csv = pl.read_csv(f,truncate_ragged_lines=True)

                conjunto_de_datos = conjunto_de_datos.join(file_csv, left_on=[column], right_on=["clave"], how="inner")
            
                conjunto_de_datos = conjunto_de_datos.with_columns(
                    pl.col("descripción").alias(column)
                )
                conjunto_de_datos = conjunto_de_datos.drop("descripción")
          

    return  conjunto_de_datos

################################### Main #########################################

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "lrf-bigdata-08d173e4f9bf.json"


storage_client = storage.Client()


bucket = storage_client.bucket("proyecto_final_divorcios")



############################ Main Data ###################################
blob = bucket.blob("conjunto_de_datos/conjunto_de_datos_ed2023.csv")
with blob.open("r") as f:
    conjunto_de_datos = pl.read_csv(f)


############################ Read Dictionary ###################################
blob = bucket.blob("diccionario_de_datos/diccionario_datos_ed2023.csv")
with blob.open("r") as f:
    diccionario_de_datos = pl.read_csv(f)


############################ Read State ###################################
blob = bucket.blob("entidad_municipio_localidad_2022.csv")
with blob.open("r") as f:
    entidades = pl.read_csv(f).filter((pl.col("cve_mun") == 0) & (pl.col("cve_loc") == 0 ))["cve_ent", "nom_loc"]


############################ Read all  ###################################
ruta_carpeta = 'catalogos/'
blobs = bucket.list_blobs(prefix=ruta_carpeta)
lista = []
for blob in blobs:
    nombre_archivo = os.path.basename(blob.name)
    lista.append(nombre_archivo)

############################ Replace csv an clean columns  ###################################

lista_archivos = [archivo.replace('.csv', '') for archivo in lista]

lista_columns = ["año_nacimiento", "año_ejecutoria", "dia", "año_sentencia", "año_registro", "edad", "numero_de_hijos", "duracion_matrimonio"]

for rem in lista_columns:
    lista_archivos.remove(rem)


############################ Execute Function And Join to obtain the State  ###################################



conjunto_de_datos = do_joins_to_main(lista_archivos, conjunto_de_datos).join(entidades, left_on="ent_mat", right_on="cve_ent", how="inner").with_columns(
    pl.col("nom_loc").alias("ent_mat")
    
).drop("nom_loc")




conjunto_de_datos = conjunto_de_datos.to_pandas()


to_gbq(conjunto_de_datos, destination_table='lrf-bigdata.divorcios.conjunto_datos', project_id='lrf-bigdata', if_exists='replace')