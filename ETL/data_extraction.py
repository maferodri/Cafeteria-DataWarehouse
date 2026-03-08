import pandas as pd

import pandas as pd

def extraccion(engine):
    print("\n============================================================")
    print("     PASO 1: EXTRACCIÓN DE LOS DATOS       ")
    print("==============================================================")

    while True: 
        print("\nOpciones para extraer los datos: ")
        print("1. Seleccionar una tabla de la base de datos")
        print("2. Ingresar una consulta SQL personalizada")

        opcion = input("\nSelecciona una opción (1 o 2): ").strip()

        if opcion == '2':
            query = input("Introduce tu consulta SQL: ")
            try:
                df = pd.read_sql(query, engine)
                print("Datos extraídos exitosamente.")
                print(f"\n {df.head()}")
                datos = len(df)
                print(f"\nLa cantidad de datos obtenidos fueron: {datos}")
                return df 
            except Exception as e: 
                print(f"Error en la consulta: {e}")
            
        elif opcion == '1':
            try: 
                # 1. Obtener lista de tablas
                query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
                tablas_df = pd.read_sql(query_tables, engine)
                lista_tablas = tablas_df['TABLE_NAME'].tolist()

                print("\nTablas disponibles: ")
                print(tablas_df['TABLE_NAME'].to_string(index=False))
                
                # --- CICLO PARA VALIDAR TABLA ---
                while True: 
                    tabla_seleccionada = input("\nEscribe el nombre de la tabla: ").strip()
                    if tabla_seleccionada in lista_tablas:
                        query_cols = f"SELECT TOP 0 * FROM {tabla_seleccionada}"
                        columnas = pd.read_sql(query_cols, engine).columns.tolist()
                        break 
                    else:
                        print(f"La tabla '{tabla_seleccionada}' no es válida.")

                # --- CICLO PARA VALIDAR CAMPOS ---
                while True:
                    print(f"\nCampos disponibles en '{tabla_seleccionada}':")
                    print(", ".join(columnas))
                    
                    campos = input("\nEscribe los campos (separados por coma) o '*' para todos: ").strip()
                    
                    try:
                        query_final = f"SELECT {campos} FROM {tabla_seleccionada}"
                        df = pd.read_sql(query_final, engine)
                        print(f"Datos de '{tabla_seleccionada}' extraídos exitosamente. Los primeros 5 datos: ")
                        print(f"\n {df.head()}")
                        datos = len(df)
                        print(f"\nLa cantidad de datos obtenidos fueron: {datos}")
                        return df # Retornamos el DataFrame final
                    
                    except Exception as e:
                        print(f"Error en los campos: {e}. Intenta de nuevo.")

            except Exception as e:
                print(f"Error general: {e}")
        else:
            print("Opción Inválida. Intente nuevamente.")