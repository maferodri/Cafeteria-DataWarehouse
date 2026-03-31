import pandas as pd

def data_load(df_conv, table_destination, engine):
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
    
    df_final = df_destination
    
    print("COLUMNAS DISPONIBLES ---->")
    for column in columns_conv:
        print(column)
    print("")
    
    column_pair = []
    
    for column in columns_destination:
        print(f"Eliga la columna para {column}")
        
        while True:
            col_select = input("\nIngrese el nombre: ")
            if col_select not in columns_conv:
                print("No existe esa columna")
            else:
                break
        
        column_pair.append(col_select)
        df_final[column] = df_conv[col_select]
    
    print("\nESTE SERIA EL EMPAREJAMIENTO FINAL")
    for i in range(len(column_pair)):
        print(f"{column_pair[i]}  ---->   {columns_destination[i]}")
            
    
    df_final.to_sql(
    name=table_destination,
    con=engine,
    schema='dbo',
    if_exists='append',
    index=False
    )
    
    print("INSERCION COMPLETADA !!!!!!!!!!!")
    
    