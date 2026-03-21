import pandas as pd
from sqlalchemy import create_engine

from data_conversion import menu_conversion

def obtener_conexion():
    try:
        server = 'localhost,1433' 
        database = 'BD_CAFETERIA' 
        username = 'sa'
        password = input("Dime tu contraseña para SQL Server: ")

        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            'Encrypt=no;'
            'TrustServerCertificate=yes;'
        )
        
        connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"
        engine = create_engine(connection_url)
        
        return engine
        
    except Exception as e:
        print(f"Error al conectar: {e}")
        return None

def extraer_datos():
    engine = obtener_conexion()
    if engine:
        # try:
        #     query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        #     df = pd.read_sql(query, engine)
        #     print("///////////////////////////////////////////////////////////////////")
        #     print("SE BIENVENIDO A NUESTRO ETL")
        #     print("Porfavor indica el numero de tabla de la cual deseas extraer los datos---------> \n")
        #     print(df.head())
            
        #     data_conversion(engine)
            
        # except Exception as e:
        #     print(f"Error al extraer los datos de la tabla de origen:{e}")
        # finally:
        #     engine.dispose()
        
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        df = pd.read_sql(query, engine)
        # print("///////////////////////////////////////////////////////////////////")
        # print("SE BIENVENIDO A NUESTRO ETL")
        # print("Porfavor indica el numero de tabla de la cual deseas extraer los datos---------> \n")
        # print(df.head())
            
        menu_conversion(engine)
        
        
        return df
    