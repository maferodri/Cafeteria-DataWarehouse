import pandas as pd

def data_conversion (engine) :
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    df = pd.read_sql(query, engine)
    
    #table_num = int(input("\nIndica el numero de la tabla que deseas: "))
    table_name = df.iloc[0]['TABLE_NAME']
            
    table_query = f"SELECT * FROM {table_name}"
            
    table_df = pd.read_sql(table_query, engine)
    print('\nEjemplo de los primeros 5 registros de la tabla seleccionada')
    print(table_df.head()) 
    
    
    print("\n//////// BIENVENIDO AL DATA CONVERSION /////////////// \n")
    print("\nBien!! Ahora que cambio le quieres hacer? Selecciona el numero")
    print("\n1: Convertir a Minuscula")
    print("2: Convertir a Mayuscula")
    print("3: Obtener parte de fecha")
    print("4: Concatenar Campos")
    
    opt = input("\nSelecciona un numero: ")
    
    
    print("\nPorfavor seleccione el numero y la columna que desea editar")
    
    columnas = table_df.columns.tolist()
    
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    column_conversion = input("Nombre de la columna: ")