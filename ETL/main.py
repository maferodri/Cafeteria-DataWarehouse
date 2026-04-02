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
    
    while True: 
        df_extraccion = extraccion(engine_origen)

        if df_extraccion is not None:

            while True:
                tabla_dest, cols_dest = seleccionar_destino(engine_destino)

                if not tabla_dest:
                    break

                df_carga = limpiar_datos(df_extraccion, engine_destino, tabla_dest)

                if isinstance(df_carga, str) and df_carga == "WRONG_TABLE":
                    print("\nPor favor elige una tabla destino compatible.\n")
                    continue
                else:
                    break

            if df_carga is None:
                print("\nNo hay registros nuevos para insertar.")
                continuar = input("\n¿Desea cargar otra tabla? (s/n): ").strip().lower()
                if continuar != 's':
                    print('\nEl ETL finalizado correctamente')
                    break
                else:
                    continue

            elif isinstance(df_carga, str) and df_carga == "WRONG_TABLE":
                continue

            else:
                result = 0
                df_mod = df_carga
                while result == 0:    
                    if df_mod is not None:
                        df_conv = data_conversion(df_mod, df_carga.columns.tolist())
                        if df_conv is not None:
                            result, df_mod = data_load(df_conv, tabla_dest, engine_destino)
                if result == -2:
                    print("\nRegresando al Paso 1...")
                    continue  
                elif result == 1:
                    print("\nProceso finalizado exitosamente.")
                elif result == -1:
                    print("\nEl proceso finalizó con errores en la inserción.")

        else:
            break

        continuar = input("\n¿Desea cargar otra tabla? (s/n): ").strip().lower()
        if continuar != 's':
            print('\nEl ETL finalizado correctamente')
            break


if __name__ == "__main__":
    main()