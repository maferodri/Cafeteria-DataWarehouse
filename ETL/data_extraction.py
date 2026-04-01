import pandas as pd

engine_oltp = 'BD_CAFETERIA'

def extraccion(engine_oltp):
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
                tablas_df = pd.read_sql(query_tables, engine_oltp)

                print("\n==============================================================")
                print("            CONSULTA SQL PERSONALIZADA            ")
                print("==============================================================")

                print("\nOpciones: ")
                print("0. Menú Principal")

                volver = input("\nSi deseas volver al menu principal presiona 0, de lo contrario cualquier tecla: ")

                if volver == '0':
                    break
                
                print("\nLas tablas con sus campos son: ")

                for tabla in tablas_df['TABLE_NAME']:
                    query_columns = f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = '{tabla}'
                    """
                    columnas_df = pd.read_sql(query_columns, engine_oltp)
                    lista_campos = ", ".join(columnas_df['COLUMN_NAME'].tolist())
                    print(f"\n TABLA: {tabla} -> CAMPOS: {lista_campos}")


                query = input("\nIntroduce tu consulta SQL: ")

                if not query:
                    print("La consulta no puede estar vacía. Intenta de nuevo.")
                    continue

                try:
                    df = pd.read_sql(query, engine_oltp)

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
                    query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME != 'sysdiagrams'"
                    tablas_df = pd.read_sql(query_tables, engine_oltp)
                    lista_tablas = tablas_df['TABLE_NAME'].tolist()

                    print("\n==============================================================")
                    print("            SELECCIONAR TABLA DE LA BASE DE DATOS            ")
                    print("==============================================================")

                    print("\nOpciones: ")
                    print("0. Menú Principal")

                    volver = input("Si deseas volver al menu principal presiona 0, de lo contrario cualquier tecla: ")

                    if volver == '0':
                        break

                    print("\nTablas disponibles en BD OLTP:")
                    for i, tabla in enumerate(lista_tablas, 1):
                        print(f"{i}. {tabla}")
                    
                    while True: 
                        entrada = input("\nEscribe el número de la tabla: ").strip()
                        if entrada.isdigit() and 1 <= int(entrada) <= len(lista_tablas):
                            tabla_seleccionada = lista_tablas[int(entrada) - 1]
                            query_cols = f"SELECT TOP 0 * FROM {tabla_seleccionada}"
                            columnas = pd.read_sql(query_cols, engine_oltp).columns.tolist()
                            print(f"\nTabla seleccionada: {tabla_seleccionada}")
                            break 
                        else:
                            print(f"\nNúmero inválido. Ingresa un número entre 1 y {len(lista_tablas)}")

                    print(f"\nCampos disponibles en '{tabla_seleccionada}':")
                    for i, col in enumerate(columnas, 1):
                        print(f"{i}. {col}")

                    while True:
                        entrada_campos = input("\nEscribe los números de los campos (separados por coma) o '*' para todos: ").strip()

                        if entrada_campos == '*':
                            campos_validados = '*'
                            break

                        numeros = [n.strip() for n in entrada_campos.split(',')]
                        todos_validos = all(n.isdigit() and 1 <= int(n) <= len(columnas) for n in numeros)

                        if todos_validos:
                            campos_validados = ", ".join(columnas[int(n) - 1] for n in numeros)
                            break
                        else:
                            print(f"\nNúmero(s) inválido(s). Ingresa números entre 1 y {len(columnas)}")
                        
                    try:
                        query_final = f"SELECT {campos_validados} FROM {tabla_seleccionada}"
                        df = pd.read_sql(query_final, engine_oltp)
                        print(f"\nDatos de '{tabla_seleccionada}' extraídos exitosamente. Los primeros 5 datos: ")
                        print(f"\n {df.head()}")
                        print(f"\nLa cantidad de datos obtenidos fueron: {len(df)}")
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