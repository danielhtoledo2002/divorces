import polars as pl 

import os 
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
              
                file_csv = pl.read_csv(f"catalogos/{name_csv[0]}.csv",truncate_ragged_lines=True)
                conjunto_de_datos = conjunto_de_datos.join(file_csv, left_on=[column], right_on=["clave"], how="inner")
              
                conjunto_de_datos = conjunto_de_datos.with_columns(
                    pl.col("descripción").alias(column)
                )
                conjunto_de_datos = conjunto_de_datos.drop("descripción")
          

    return  conjunto_de_datos

    

conjunto_de_datos = pl.read_csv("conjunto_de_datos/conjunto_de_datos_ed2023.csv")


diccionario_de_datos = pl.read_csv("diccionario_de_datos/diccionario_datos_ed2023.csv")

entidades = pl.read_csv("entidad_municipio_localidad_2022.csv").filter((pl.col("cve_mun") == 0) & (pl.col("cve_loc") == 0 ))["cve_ent", "nom_loc"]




ruta_carpeta = 'catalogos/'
lista_archivos = [archivo.replace('.csv', '') for archivo in os.listdir(ruta_carpeta) if os.path.isfile(os.path.join(ruta_carpeta, archivo))]

lista_columns = ["año_nacimiento", "año_ejecutoria", "dia", "año_sentencia", "año_registro", "edad", "numero_de_hijos", "duracion_matrimonio"]
# "edad_div1", "nacim_div1", "pat_hij", "cus_hij", "hij_men", "hijos", "anio_eje", "anio_mat", "dia_reg", "anio_reg", "dia_eje", "edad_div2", "nacim_div2", 
for rem in lista_columns:
    lista_archivos.remove(rem)

conjunto_de_datos = do_joins_to_main(lista_archivos, conjunto_de_datos).join(entidades, left_on="ent_mat", right_on="cve_ent", how="inner").with_columns(
    pl.col("nom_loc").alias("ent_mat")
    
).drop("nom_loc")


print(conjunto_de_datos)