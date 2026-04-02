import pandas as pd
import re

from pandas.api.types import is_datetime64_any_dtype

def data_conversion (table_df, table_original) :
    
    
    print('\nEjemplo de los primeros 5 registros de la tabla seleccionada')
    print(table_df.head()) 
    
    print("\n//////// BIENVENIDO AL DATA CONVERSION /////////////// \n")

    #Forzar a cambiar los tipos de datos que se regreso de la DB
    table_df = table_df.convert_dtypes()
    
    while True:
        print("\nBien!! Ahora que cambio le quieres hacer? Selecciona el numero")
        print("\n1: Convertir a Minuscula")
        print("2: Convertir a Mayuscula")
        print("3: Obtener parte de fecha")
        print("4: Concatenar Campos")
        print("5: Eliminar Columna")
        print("6: Cambiar tipo de dato (Texto a Fecha)")
        print("7: Vista Previa de las tablas")
        print("8: Salir del Menu Conversion")
        
        opt = input("\nSelecciona un numero: ")
        
        if opt == "1":
            table_df = conversion_minuscula(table_df, table_original)
        elif opt == "2":
            table_df = conversion_mayuscula(table_df, table_original)
        elif opt == "3":
            table_df = extraer_fecha(table_df, table_original)
        elif opt == "4":
            table_df = concatenar_campos(table_df, table_original)
        elif opt == "5":
            table_df = eliminar_campo(table_df, table_original)
        elif opt == "6":
            table_df = cambiar_dato(table_df, table_original)
        elif opt == "7":
            print("Vista Previa de toda la tabla")
            print(table_df.head())
        elif opt == "8":
            return table_df
        else:
            print("No esta dentro de las opciones mencionadas")
    
    
    
    
    
    
def conversion_minuscula (table_df, table_original):
    print("\nPorfavor seleccione el numero de la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    while True:
        column_conversion = input("\nNombre de la columna: ")
        if column_conversion in table_original:
            if table_df[column_conversion].dtype != "string":
                print("La columna seleccionada no es un tipo de dato admitido")
                #return
            else:
                break;
        else:
            print("La columna no existe")
    
    while True:
        column_add = input("Escriba el nombre de la nueva tabla en donde se guardaran los datos: ")
        if column_add in table_original:
            print("Ya existe una tabla con ese nombre")
        else:
            break;
    
    table_df[column_add] = ""
    table_df[column_add] = table_df[column_conversion].str.lower()
    
    return table_df



def conversion_mayuscula (table_df, table_original):
    print("\nPorfavor seleccione el numero de la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    while True:
        column_conversion = input("\nNombre de la columna: ")
        
        if column_conversion in table_original:
            if table_df[column_conversion].dtype != "string":
                print("La columna seleccionada no es un tipo de dato admitido")
                #return;
            else:
                break;
        else:
            print("La columna no existe")
    
    while True:
        column_add = input("Escriba el nombre de la nueva tabla en donde se guardaran los datos: ")
        if column_add in table_original:
            print("Ya existe una tabla con ese nombre")
        else:
            break;
    
    table_df[column_add] = ""
    table_df[column_add] = table_df[column_conversion].str.upper()
    
    return table_df



def extraer_fecha (table_df, table_original):
    print("\nPorfavor seleccione el numero y la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    while True:
        column_conversion = input("\nNombre de la columna: ")
        insert_conversion = column_conversion
        is_date_time = pd.to_datetime(table_df[column_conversion], errors='coerce', format='mixed').notna().all()
        
        if column_conversion in table_original:
            if not (is_date_time) or table_df[column_conversion].dtype == 'Int64':
                print("La columna seleccionada no es un tipo de dato admitido")
            else:
                print(table_df[column_conversion].dtype)
                if table_df[column_conversion].dtype == "string":
                    insert_conversion = "DT_" + column_conversion
                    print("El campo seleccionado no es una fecha, pero se creara una columna para ser usada")
                    print("Nombre de la nueva columna: " + insert_conversion)
                break;
        else:
            print("La columna no existe, vuelva a ingresar el nombre de la columna")
    try:
        table_df[insert_conversion] = pd.to_datetime(table_df[column_conversion], format='mixed')
    except Exception as e:
        print("El formato de tipo string no es posible convertirlo a date")
    
    print("\nQue desea obtener de la fecha?")
    print("1. Año")
    print("2. Semestre")
    print("3. Trimestre")
    print("4. Mes")
    print("5. Dia")
    print("6. hora")
    print("7. AM o PM")
    
    opciones = ["1", "2", "3", "4", "5", "6"]
    
    while True:
        opt = input("\nSelecciona un numero: ")
        if opt not in opciones:
            print("No esta dentro de las opciones")
        else:
            break
    
    print("\nElige la nueva columna donde se ingresara el dato")
    print("----Columnas Existentes")
    for column in columnas:
        print(column) 
    
    while True:
        column_date = input("Ingresa el de la nueva columna: ")
        if column_date in table_original:
            print("La columna ya existe")
        else:
            break
    
    if opt == "1":
        table_df[column_date] = table_df[insert_conversion].dt.year
    elif opt == "2":
        table_df[column_date] = (table_df[column_date].dt.quarter - 1) // 2 + 1
    elif opt == "3":
        table_df[column_date] = table_df[insert_conversion].dt.quarter
    elif opt == "4":
        table_df[column_date] = table_df[insert_conversion].dt.month_name()
    elif opt == "5":
        table_df[column_date] = table_df[insert_conversion].dt.day
    elif opt == "6":
        table_df[column_date] = table_df[insert_conversion].dt.hour
    elif opt == "7":
        table_df[column_date] = table_df.apply(lambda x: "AM" if x[insert_conversion].hour < 12 else "PM", axis=1)
    
    return table_df



def concatenar_campos (table_df, table_original):
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
    
    return table_df



def eliminar_campo(table_df, table_original):
    print("/////ELIMINAR UNA COLUMNA O CAMPO")
    print("\nColumnas Disponibles a eliminar")
    columnas_originales = table_original
    columnas = table_df.columns.tolist()
    for columna in columnas:
        if (columna not in columnas_originales):
            print(columna)
    while True:
        column_delete = input("Seleccciona la columna que sera eliminada: ")
        if column_delete not in columnas or column_delete in columnas_originales:
            print("La columna no existe o no puede ser cambiada")
            return table_df   
        else:
            table_df = table_df.drop(columns=[column_delete])
            break 
    
    return table_df









def cambiar_dato(table_df, table_original):
    print("\nPorfavor seleccione el numero y la columna que desea editar")
    columnas = table_df.columns.tolist()
    print("----------------Columnas ------->")
    for column in columnas:
        print(column) 
    
    while True:
        column_conversion = input("\nNombre de la columna: ")
        insert_conversion = column_conversion
        is_date_time = pd.to_datetime(table_df[column_conversion], errors='coerce', format='mixed').notna().all()
        
        if column_conversion in table_original:
            if not (is_date_time) or table_df[column_conversion].dtype == 'Int64':
                print("La columna seleccionada no es un tipo de dato admitido")
                return table_df;
            else:
                insert_conversion = input("Escriba el nombre de la nueva columna: ")
                for column in table_original:
                    if column == insert_conversion:
                        print("La columna ya esta dentro de la base de datos")
                        return table_df
                break;
        else:
            print("La columna no existe")
            return table_df;
    try:
        table_df[insert_conversion] = pd.to_datetime(table_df[column_conversion], format='mixed')
    except Exception as e:
        print("El formato de tipo string no es posible convertirlo a date")
    return table_df
    