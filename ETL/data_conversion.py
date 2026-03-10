import pandas as pd
import re

def data_conversion (engine) :
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    df = pd.read_sql(query, engine)
    
    #table_num = int(input("\nIndica el numero de la tabla que deseas: "))
    table_name = df.iloc[4]['TABLE_NAME']
            
    table_query = f"SELECT * FROM {table_name}"
            
    table_df = pd.read_sql(table_query, engine)
    table_original = table_df.df.tolist()
    print('\nEjemplo de los primeros 5 registros de la tabla seleccionada')
    print(table_df.head()) 
    
    
    print("\n//////// BIENVENIDO AL DATA CONVERSION /////////////// \n")
    print("\nBien!! Ahora que cambio le quieres hacer? Selecciona el numero")
    print("\n1: Convertir a Minuscula")
    print("2: Convertir a Mayuscula")
    print("3: Obtener parte de fecha")
    print("4: Concatenar Campos")
    print("5: Eliminar Columna")
    
    #opt = input("\nSelecciona un numero: ")
    opt = "5"
    
    if opt == "1":
        conversion_minuscula(engine, table_df, table_original)
    elif opt == "2":
        conversion_mayuscula(engine, table_df, table_original)
    elif opt == "3":
        extraer_fecha(engine, table_df, table_original)
    elif opt == "4":
        concatenar_campos(engine, table_df, table_original)
    elif opt == "5":
        eliminar_campo(engine, table_df, table_original)
    else:
        print("No esta dentro de las opciones mencionadas")
    
    
    
    
    
    
def conversion_minuscula (engine, table_df, table_original):
    print("\nPorfavor seleccione el numero de la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    column_conversion = input("Nombre de la columna: ")
    if table_df[column_conversion].dtype != "str":
        print("La columna seleccionada no es un tipo de dato admitido")
        return;
    
    column_add = input("Escriba el nombre de la nueva tabla en donde se guardaran los datos")
    if column_add in table_original:
        print("Ya existe una tabla con ese nombre")
    
    table_df[column_add] = ""
    table_df[column_add] = table_df[column_conversion].str.lower()
    
    print("Tabla con los cambios hechos")
    print(table_df.head())



def conversion_mayuscula (engine, table_df, table_original):
    print("\nPorfavor seleccione el numero y la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    column_conversion = input("Nombre de la columna: ")
    if table_df[column_conversion].dtype != "str":
        print("La columna seleccionada no es un tipo de dato admitido")
        return;
    
    column_add = input("Escriba el nombre de la nueva tabla en donde se guardaran los datos")
    if column_add in table_original:
        print("Ya existe una tabla con ese nombre")
    
    table_df[column_add] = ""
    table_df[column_add] = table_df[column_conversion].str.upper()
    
    print("Tabla con los cambios hechos")
    print(table_df.head())



def extraer_fecha (engine, table_df, table_original):
    print("\nPorfavor seleccione el numero y la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    column_conversion = input("Nombre de la columna: ")
    if table_df[column_conversion].dtype != "object":
        print("La columna seleccionada no es un tipo de dato admitido")
        return;
    
    table_df[column_conversion] = pd.to_datetime(table_df[column_conversion])
    
    print("\nQue desea obtener de la fecha?")
    print("1. Año")
    print("2. Mes")
    print("3. Dia")
    print("4. hora")
    
    opt = input("\nSelecciona un numero: ")
    
    print("\nElige la columna en la que ingresaras el dato, o crea una nueva")
    print("----Columnas Existentes")
    for column in columnas:
        print(column) 
    
    column_date = input("Ingresa el de la nueva columna: ")
    if column_date in table_original:
        print("La columna ya existe")
    
    if opt == "1":
        table_df[column_date] = table_df[column_conversion].dt.year
    elif opt == "2":
        table_df[column_date] = table_df[column_conversion].dt.month_name()
    elif opt == "3":
        table_df[column_date] = table_df[column_conversion].dt.day
    elif opt == "4":
        table_df[column_date] = table_df[column_conversion].dt.hour
        
    print("Muestra de como va la tabla")
    print(table_df.head())



def concatenar_campos (engine, table_df, table_original):
    print("Porfavor en un ingresa la concatenacion deseada y mete dentro de corchetes [] la tabla deseada")
    print("----------------Columnas Disponibles ------->")
    for column in table_original:
        print(column) 
    concatenacion = input("Ingresala aqui: ")
    
    text_concat = re.split(r"(?=\[)|(?<=\])", concatenacion)
    
    
    column_insert_concat = input("\nIngresa el nombre de la nueva tabla en donde se agregara: ")
    
    for column in table_original:
        if column == column_insert_concat:
            print("La columna ya esta dentro de la base de datos")
    
    
    table_df[column_insert_concat] = ""
    
    for part in text_concat:
        if "[" in part and "]" in part:
            column_exist = part.replace("[", "").replace("]","")
            
            table_df[column_insert_concat] = table_df[column_insert_concat] + table_df[column_exist].astype(str)
        
        else:
            table_df[column_insert_concat] = table_df[column_insert_concat] + part    
            
    print(table_df.head())

def eliminar_campo(engine, table_df, table_original):
    print("/////ELIMINAR UNA COLUMNA O CAMPO")
    print("\nColumnas Disponibles a eliminar")
    columnas_originales = table_original.columns.tolist()
    columnas = table_df.columns.tolist()
    for columna in columnas:
        if (columna not in columnas_originales):
            print(columna)
    column_delete = input("Seleccciona la columna que sera eliminada: ")
    
    