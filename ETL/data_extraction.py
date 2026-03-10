import pandas as pd

def extraccion(engine):
    print("\n==============================================================")
    print("            PASO 1: EXTRACCIÓN DE LOS DATOS            ")
    print("==============================================================")

    while True: 
        print("\nOpciones para extraer los datos: ")
        print("1. Seleccionar una tabla de la base de datos")
        print("2. Ingresar una consulta SQL personalizada")
        print("3. Salir")

        opcion = input("\nSelecciona una opción: ").strip()

        if opcion == '2':
            while True: 
                query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
                tablas_df = pd.read_sql(query_tables, engine)

                print("\n==============================================================")
                print("            CONSULTA SQL PERSONALIZADA            ")
                print("==============================================================")

                volver = input("Si deseas volver al menu principal presiona 0, de lo contrario cualquier tecla: ")

                if volver == '0':
                    break
                
                print("\nLas tablas con sus campos son: ")

                for tabla in tablas_df['TABLE_NAME']:
                    query_columns = f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = '{tabla}'
                    """
                    columnas_df = pd.read_sql(query_columns, engine)
                    lista_campos = ", ".join(columnas_df['COLUMN_NAME'].tolist())
                    print(f"\n TABLA: {tabla} -> CAMPOS: {lista_campos}")


                query = input("Introduce tu consulta SQL: ")
                try:
                    df = pd.read_sql(query, engine)

                    if not df.empty:
                        print("\nDatos extraídos exitosamente. Los primeros 5 fueron: ")
                        print(f"\n {df.head()}")
                        datos = len(df)
                        print(f"\nLa cantidad de datos obtenidos fueron: {datos}")
                        return df 
                    else: 
                        print("\nLa consulta no devolvio ningun resultado. Verifique la consulta e intente nuevamente")

                except Exception as e: 
                    print(f"\nError en la consulta: {e}")
            
        elif opcion == '1':
            while True:
                try: 
                    query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
                    tablas_df = pd.read_sql(query_tables, engine)
                    lista_tablas = tablas_df['TABLE_NAME'].tolist()

                    print("\n==============================================================")
                    print("            SELECCIONAR TABLA DE LA BASE DE DATOS            ")
                    print("==============================================================")

                    volver = input("Si deseas volver al menu principal presiona 0, de lo contrario cualquier tecla: ")

                    if volver == '0':
                        break

                    print("\nTablas disponibles: ")
                    print(tablas_df['TABLE_NAME'].to_string(index=False))
                    
                    while True: 
                        tabla_seleccionada = input("\nEscribe el nombre de la tabla: ").strip()
                        if tabla_seleccionada in lista_tablas:
                            query_cols = f"SELECT TOP 0 * FROM {tabla_seleccionada}"
                            columnas = pd.read_sql(query_cols, engine).columns.tolist()
                            break 
                        else:
                            print(f"\nLa tabla '{tabla_seleccionada}' no es válida. Intente nuevamente")

                    print(f"\nCampos disponibles en '{tabla_seleccionada}':")
                    print(", ".join(columnas))

                    while True:
                        entrada_campos = input("\nEscribe los campos (separados por coma) o '*' para todos: ").strip()

                        if entrada_campos == '*':
                            campos_validados = '*'
                            break

                        lista_seleccionada = [c.strip() for c in entrada_campos.split(',')]
                        campos_invalidos = [c for c in lista_seleccionada if c not in columnas]

                        if not campos_invalidos: 
                            campos_validados = ", ".join(lista_seleccionada)
                            break
                        else: 
                            print(f"\nError: Los siguientes campos no existen: {', '.join(campos_invalidos)}")
                        
                    try:
                        query_final = f"SELECT {entrada_campos} FROM {tabla_seleccionada}"
                        df = pd.read_sql(query_final, engine)
                        print(f"\nDatos de '{tabla_seleccionada}' extraídos exitosamente. Los primeros 5 datos: ")
                        print(f"\n {df.head()}")
                        datos = len(df)
                        print(f"\nLa cantidad de datos obtenidos fueron: {datos}")
                        return df 
                        
                    except Exception as e:
                        print(f"/nError en los campos: {e}. Intenta de nuevo.")

                except Exception as e:
                    print(f"Error general: {e}")

        elif opcion == '3':
            print("Saliendo de ETL...")
            break

        else:
            print("Opción Inválida. Intente nuevamente.")