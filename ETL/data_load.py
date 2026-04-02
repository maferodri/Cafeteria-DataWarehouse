import re

import pandas as pd
from sqlalchemy import inspect

def data_load(df_conv, table_destination, engine):

    #Utilizamos el inspector para encontrar el tipo de dato de SQL Server y encontrar el tamaño de los varchar de cada columna
    inspector = inspect(engine)
    col_inspector = inspector.get_columns(table_destination)
    columns_inspector = {col['name']: col for col in col_inspector}

    #Insertar los datos
    
    print("\n==========================================")
    print("CARGAR LOS DATOS")
    print("=============================================")
    print("Ahora emparejemos las tablas de la BD Origen y la BD Destino")
    
    query_destination = f"SELECT * FROM {table_destination}"
    df_destination = pd.read_sql(query_destination, engine)
    
    columns_destination = df_destination.columns.tolist()
    
    columns_conv = df_conv.columns.tolist()
    
    print("Se necesitan llenar las siguientes columnas: ")
    print(columns_destination)
    print("")

    #Igualamos las tablas (DataFrames) para que tengan las mismas columnas y no tener errores al insertar
    
    df_final = df_destination.reindex(index=[])
    
    print("COLUMNAS DISPONIBLES ---->")
    for column in columns_conv:
        print(column)
    print("")
    column_pair = []
    
    while True:

        for column in columns_destination:

            column_pair = []
            
            while True:
                print(f"\nEliga la columna para {column} o si deseas retornar al menu de conversion presiona [c]")
                col_select = input("\nIngrese el nombre: ")
                if col_select == 'c':
                    print('--------------REGRESANDO A LA CONVERSION----------------')
                    return 0, df_conv
                if col_select not in columns_conv:
                    print("No existe esa columna")
                else:
                    insp_col = columns_inspector[column];
                    # print(insp_col['type'])
                    # print(df_conv[col_select].dtype)

                    #Verificacion de datos entre SQL Server y los DataFrames ------------------
                    
                    #Verificacion de Fechas

                    tipos_permitidos = ['object', 'datetime64[ns]']
                    if ('DATE' in str(insp_col['type']).upper()) and (df_conv[col_select].dtype not in tipos_permitidos):
                        print("El tipo de datos no coincide")
                        continue

                    #Verificacion de Enteros

                    if 'INTEGER' in str(insp_col['type']) and df_conv[col_select].dtype != 'Int64':
                        print("El tipo de datos no coincide")
                        continue

                    #Verificacion de CHAR o VARCHAR

                    if 'CHAR' in str(insp_col['type']):

                        if (df_conv[col_select].dtype != 'string'):
                            print('El tipo de dato no coincide')
                            continue

                        #Verificacion de que no exceda el limite del VARCHAR

                        type_str = str(insp_col['type'])
                        match = re.search(r'\((\d+)\)', type_str)
                        max_largo = df_conv[col_select].str.len().max()
                        if match:
                            size_limit = int(match.group(1))
                            if (max_largo > size_limit):
                                print("Existe un registro con mas datos de los permitidos")
                                continue

                    break
            
            column_pair.append(col_select)
            df_final[column] = df_conv[col_select]
        
        print("\nESTE SERIA EL EMPAREJAMIENTO FINAL")
        for i in range(len(column_pair)):
            print(f"{column_pair[i]}  ---->   {columns_destination[i]}")
            
        opt_emp = input("\nSi esta de acuerdo con el emparejamiento inserte [s] o si desea regresar al menu de conversion [c]: ")
        if opt_emp == 's':
            break
        if opt_emp == 'c':
            return 0, df_conv
        else:
            print("Se volvera a hacer el emparejamiento \n")

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
        print(f"Ha ocurrido un error: {e}")
        return -1, df_conv;
    
    print("\nINSERCION COMPLETADA !!!!!!!!!!!")
    return 1, df_conv;
    
    