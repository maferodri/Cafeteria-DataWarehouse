import pandas as pd
from conexion import conexion_oltp, conexion_olap
from data_extraction import extraccion
from data_destination import seleccionar_destino
from data_cleaning import limpiar_datos
from data_conversion import data_conversion
from data_load import data_load

def main():
    print("=======================================================")
    print("           BIENVENIDO AL SISTEMA ETL                   ")
    print("=======================================================")

    while True: 
        sql_password = input("Ingresa tu contraseña SQL Server: ")
        engine_origen = conexion_oltp(sql_password)
        engine_destino = conexion_olap(sql_password)

        if engine_origen and engine_destino:
            break
        else:
            print("Contraseña incorrecta. Intente nuevamente")
    
    df_extraccion = extraccion(engine_origen)

    if df_extraccion is not None:
        tabla_dest, cols_dest = seleccionar_destino(engine_destino)

        if tabla_dest:
            df_carga = limpiar_datos(df_extraccion, engine_destino, tabla_dest)

        if df_carga is not None:
            df_conv = data_conversion(df_carga, df_carga.columns.tolist())

            if df_conv is not None:
                data_load(df_conv, tabla_dest, engine_destino)

        else:
            print("\nProceso finalizado: no hay registros nuevos para insertar.")
            
    else:
        print("Proceso finalizado")


if __name__ == "__main__":
    main()


    