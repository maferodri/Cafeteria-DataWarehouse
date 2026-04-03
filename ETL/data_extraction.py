import pandas as pd
from colorama import Fore, Style, init

# Colores
init(autoreset=True)

engine_oltp = 'BD_CAFETERIA'

def extraccion(engine_oltp):
    print(f"\n{Fore.CYAN}==============================================================")
    print(f"{Fore.CYAN}            PASO 1: EXTRACCIÓN DE LOS DATOS            ")
    print(f"{Fore.CYAN}==============================================================")

    while True: 
        print(f"\n{Fore.MAGENTA}Opciones para extraer los datos: ")
        print("1. Seleccionar una tabla de la base de datos")
        print("2. Ingresar una consulta SQL personalizada")
        print("3. Salir")

        opcion = input(f"\n{Fore.CYAN}Selecciona una opción: {Style.RESET_ALL}").strip()

        if opcion == '2':
            while True: 
                query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
                tablas_df = pd.read_sql(query_tables, engine_oltp)

                print(f"\n{Fore.CYAN}==============================================================")
                print(f"{Fore.CYAN}            CONSULTA SQL PERSONALIZADA            ")
                print(f"{Fore.CYAN}==============================================================")

                print("\nOpciones: ")
                print(f"{Fore.YELLOW}0. Menú Principal")

                volver = input(f"\nSi deseas volver al menu principal presiona {Fore.YELLOW}0{Style.RESET_ALL}, de lo contrario cualquier tecla: ")

                if volver == '0':
                    break
                
                print(f"\n{Fore.MAGENTA}Las tablas con sus campos son: ")

                for tabla in tablas_df['TABLE_NAME']:
                    query_columns = f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = '{tabla}'
                    """
                    columnas_df = pd.read_sql(query_columns, engine_oltp)
                    lista_campos = ", ".join(columnas_df['COLUMN_NAME'].tolist())
                    print(f"\n {Fore.GREEN}TABLA: {tabla}{Style.RESET_ALL} -> CAMPOS: {lista_campos}")


                query = input(f"\n{Fore.CYAN}Introduce tu consulta SQL: {Style.RESET_ALL}").strip()

                if not query:
                    print(f"{Fore.RED}La consulta no puede estar vacía. Intenta de nuevo.")
                    continue

                try:
                    df = pd.read_sql(query, engine_oltp)

                    if not df.empty:
                        print(f"\n{Fore.GREEN}Datos extraídos exitosamente. Los primeros 5 registros fueron: ")
                        print(f"\n {df.head()}")
                        datos = len(df)
                        print(f"\nLa cantidad de datos obtenidos fueron: {Fore.GREEN}{datos}")
                        return df 
                    else: 
                        print(f"\n{Fore.YELLOW}La consulta no devolvio ningun resultado. Verifique la consulta e intente nuevamente")

                except Exception as e: 
                    print(f"\n{Fore.RED}Error en la consulta: {e}")
            
        elif opcion == '1':
            while True:
                try: 
                    query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME != 'sysdiagrams'"
                    tablas_df = pd.read_sql(query_tables, engine_oltp)
                    lista_tablas = tablas_df['TABLE_NAME'].tolist()

                    print(f"\n{Fore.CYAN}==============================================================")
                    print(f"{Fore.CYAN}            SELECCIONAR TABLA DE LA BASE DE DATOS            ")
                    print(f"{Fore.CYAN}==============================================================")

                    print("\nOpciones: ")
                    print(f"{Fore.YELLOW}0. Menú Principal")

                    volver = input(f"Si deseas volver al menu principal presiona {Fore.YELLOW}0{Style.RESET_ALL}, de lo contrario presiona cualquier tecla: ")

                    if volver == '0':
                        break

                    print(f"\n{Fore.MAGENTA}Tablas disponibles en BD OLTP:")
                    for i, tabla in enumerate(lista_tablas, 1):
                        print(f"{Fore.WHITE}{i}. {tabla}")
                    
                    while True: 
                        entrada = input(f"\n{Fore.CYAN}Escribe el número de la tabla: {Style.RESET_ALL}").strip()
                        if entrada.isdigit() and 1 <= int(entrada) <= len(lista_tablas):
                            tabla_seleccionada = lista_tablas[int(entrada) - 1]
                            query_cols = f"SELECT TOP 0 * FROM {tabla_seleccionada}"
                            columnas = pd.read_sql(query_cols, engine_oltp).columns.tolist()
                            print(f"\n{Fore.GREEN}Tabla seleccionada: {tabla_seleccionada}")
                            break 
                        else:
                            print(f"\n{Fore.RED}Número inválido. Ingresa un número entre 1 y {len(lista_tablas)}")

                    print(f"\nCampos disponibles en '{Fore.GREEN}{tabla_seleccionada}{Style.RESET_ALL}':")
                    for i, col in enumerate(columnas, 1):
                        print(f"{i}. {col}")

                    while True:
                        entrada_campos = input(f"\n{Fore.CYAN}Escribe los números de los campos (separados por coma) o '*' para todos: {Style.RESET_ALL}").strip()

                        if entrada_campos == '*':
                            campos_validados = '*'
                            break

                        numeros = [n.strip() for n in entrada_campos.split(',')]
                        todos_validos = all(n.isdigit() and 1 <= int(n) <= len(columnas) for n in numeros)

                        if todos_validos:
                            campos_validados = ", ".join(columnas[int(n) - 1] for n in numeros)
                            break
                        else:
                            print(f"\n{Fore.RED}Número(s) inválido(s). Ingresa números entre 1 y {len(columnas)}")
                        
                    try:
                        query_final = f"SELECT {campos_validados} FROM {tabla_seleccionada}"
                        df = pd.read_sql(query_final, engine_oltp)
                        print(f"\n{Fore.GREEN}Datos de '{tabla_seleccionada}' extraídos exitosamente. Los primeros 5 registros datos: ")
                        print(f"\n {df.head()}")
                        print(f"\nLa cantidad de datos obtenidos fueron: {Fore.GREEN}{len(df)}")
                        return df 
                        
                    except Exception as e:
                        print(f"{Fore.RED}/nError en los campos: {e}. Intenta de nuevo.")

                except Exception as e:
                    print(f"{Fore.RED}Error general: {e}")

        elif opcion == '3':
            print(f"{Fore.YELLOW}Saliendo de ETL...")
            break

        else:
            print(f"{Fore.RED}Opción Inválida. Intente nuevamente.")