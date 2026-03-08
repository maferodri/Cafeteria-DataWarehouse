import pandas as pd
from sqlalchemy import create_engine

from data_conversion import data_conversion

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
    

def conexion():
    # 1. Obtenemos el motor (pide contraseña aquí)
    engine = obtener_conexion() 
    database = 'BD_CAFETERIA'
    
    if engine:
        print("\n=======================================================")
        print("\n           SE BIENVENIDO A NUESTRO ETL                   ")
        print(f"\n    Estas conectado a la Base de Datos: {database}    ")
        
        return engine  
    else:
        return None
