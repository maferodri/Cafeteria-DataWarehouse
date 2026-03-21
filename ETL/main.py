import pandas as pd
from conexion import conexion_oltp, conexion_olap
from data_extraction import extraccion
from data_destination import seleccionar_destino
from data_cleaning import limpiar_datos

def main():
    print("=======================================================")
    print("           BIENVENIDO AL SISTEMA ETL                   ")
    print("=======================================================")

    sql_password = input("Ingresa tu contraseña SQL Server: ")
    engine_origen = conexion_oltp(sql_password)
    engine_destino = conexion_olap(sql_password)

    if engine_origen and engine_destino:
        df_extraccion = extraccion(engine_origen)

        if df_extraccion is not None:
            tabla_dest, cols_dest = seleccionar_destino(engine_destino)

        if tabla_dest:
            df_carga = limpiar_datos(df_extraccion, engine_destino, tabla_dest, cols_dest)

        if df_carga is not None:
            print(f"\nTabla: {tabla_dest}")
            print("ESTADO: Datos limpiados y transformados correctamente")
            print(f"REGISTROS LISTOS: {len(df_carga)}")


if __name__ == "__main__":
    main()


    