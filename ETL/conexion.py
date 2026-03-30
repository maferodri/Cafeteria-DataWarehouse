import pandas as pd
from sqlalchemy import create_engine

def obtener_conexion(nombre_bd, password):
    try:
        server = 'localhost,1433' 
        username = 'sa'

        connection_string = (
            f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={nombre_bd};'
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
    

def conexion_oltp(password):
    database = 'BD_CAFETERIA'
    engine = obtener_conexion(database, password) 
    return engine
    
def conexion_olap(password):
    database = 'DW_CAFETERIA'
    engine = obtener_conexion(database, password)
    return engine


