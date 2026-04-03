import re
import pandas as pd
from sqlalchemy import inspect
from colorama import Fore, Style, init

# Colores
init(autoreset=True)

def data_load(df_conv, table_destination, engine):

    #Utilizamos el inspector para encontrar el tipo de dato de SQL Server y encontrar el tamaño de los varchar de cada columna
    inspector = inspect(engine)
    col_inspector = inspector.get_columns(table_destination)
    columns_inspector = {col['name']: col for col in col_inspector}

    #Insertar los datos
    
    print(f"\n{Fore.CYAN}==========================================")
    print(f"{Fore.CYAN}         PASO 5: CARGAR LOS DATOS")
    print(f"{Fore.CYAN}=============================================")
    print("Ahora emparejemos las tablas de la BD Origen y la BD Destino")
    
    query_destination = f"SELECT * FROM {table_destination}"
    df_destination = pd.read_sql(query_destination, engine)
    
    columns_destination = df_destination.columns.tolist()
    
    columns_conv = df_conv.columns.tolist()
    
    print(f"\n{Fore.YELLOW}Se necesitan llenar las siguientes columnas: ")
    print(columns_destination)
    print("")

    #Igualamos las tablas (DataFrames) para que tengan las mismas columnas y no tener errores al insertar
    
    df_final = df_destination.reindex(index=[])
    
    print(f"{Fore.MAGENTA}COLUMNAS DISPONIBLES ---->")
    for column in columns_conv:
        print(column)
    print("")
    column_pair = []
    
    while True:
        #Mafer: quite el column_pair del for porque no se imprimia bien
        column_pair = []
        for column in columns_destination:

            while True:
                #Mafer: cuando no esta la columa para emparejar regresar a la conversion no nos sirve, 
                #lo que nos sirve es dejarlo en null o volver al paso 1. 
                print(f"\nElija la columna para {Fore.GREEN}{column}{Style.RESET_ALL} o presiona {Fore.YELLOW}[s]{Style.RESET_ALL} para dejar en NULL, {Fore.YELLOW}[c]{Style.RESET_ALL} para regresar al paso 1")
                col_select = input(f"{Fore.CYAN}Ingrese el nombre: ")
                if col_select == 'c':
                    print(f'{Fore.RED}--------------REGRESANDO AL PASO 1----------------')
                    return -2, df_conv
                if col_select == 's':
                    print(f"{Fore.YELLOW}{column} quedará como NULL")
                    df_final[column] = None
                    column_pair.append('NULL')
                    break
                if col_select not in columns_conv:
                    print(f"{Fore.RED}No existe esa columna")
                else:

                    insp_col = columns_inspector[column];

                    #Verificacion de datos entre SQL Server y los DataFrames ------------------
                    
                    #Verificacion de Fechas

                    #Mafer 
                    if 'DATE' in str(insp_col['type']).upper():
                        col_dtype = str(df_conv[col_select].dtype)
                        if not (col_dtype.startswith('datetime64') or col_dtype == 'object'):
                            print(f"{Fore.RED}El tipo de datos no coincide (Se esperaba Fecha)")
                            continue

                    #Verificacion de Enteros

                    #Mafer: Habia problemas al insertar semestre, tri y asi 
                    tipos_entero_sql = ['INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'TINYINT']
                    if any(t in str(insp_col['type']).upper() for t in tipos_entero_sql):
                        col_dtype = str(df_conv[col_select].dtype)
                        if not (col_dtype.startswith('int') or col_dtype.startswith('Int')):
                            print(f"{Fore.RED}El tipo de datos no coincide (Se esperaba Entero)")
                            continue

                    #Verificacion de CHAR o VARCHAR

                    if 'CHAR' in str(insp_col['type']):

                        if (df_conv[col_select].dtype != 'string'):
                            print(f'{Fore.RED}El tipo de dato no coincide (Se esperaba String/Char)')
                            continue

                        #Verificacion de que no exceda el limite del VARCHAR

                        type_str = str(insp_col['type'])
                        match = re.search(r'\((\d+)\)', type_str)
                        max_largo = df_conv[col_select].str.len().max()
                        if match:
                            size_limit = int(match.group(1))
                            if (max_largo > size_limit):
                                print(f"{Fore.RED}Existe un registro con mas datos de los permitidos (Máx: {size_limit})")
                                continue

                    break
            
            column_pair.append(col_select)
            df_final[column] = df_conv[col_select]
        
        print(f"\n{Fore.CYAN}ESTE SERIA EL EMPAREJAMIENTO FINAL")
        for i in range(len(column_pair)):
            print(f"{Fore.YELLOW}{column_pair[i]}  {Fore.WHITE}---->   {Fore.GREEN}{columns_destination[i]}")
            
        #Mafer: poner la opcion de que si lo pueden rehacer    
        opt_emp = input(f"\n¿De acuerdo? {Fore.GREEN}[s]{Style.RESET_ALL} / rehacer {Fore.YELLOW}[r]{Style.RESET_ALL} / conversion {Fore.YELLOW}[c]{Style.RESET_ALL}: ")
        if opt_emp == 's':
            break
        if opt_emp == 'c':
            return 0, df_conv
        else:
            print(f"{Fore.MAGENTA}Se volvera a hacer el emparejamiento \n")

    #Insercion de los datos en SQL Server   
    try:
        df_final.to_sql(
        name=table_destination,
        con=engine,
        schema='dbo',
        if_exists='append',
        index=False
        )
    except Exception as e:
        print(f"{Fore.RED}Ha ocurrido un error en la inserción: {e}")
        return -1, df_conv;
    
    print(f"\n{Fore.GREEN}{Style.BRIGHT}INSERCION COMPLETADA !!!!!!!!!!!")
    return 1, df_conv;