import pandas as pd
from conexion import conexion, obtener_conexion
from data_extraction import extraccion

def main():
    ##Bienvenida y establecer la BD que estan conectados
    df_conexion = conexion()
    if df_conexion:
        df_extraccion = extraccion(df_conexion)

        if df_extraccion is not None: 
            print("Extraccion completa")

if __name__ == "__main__":
    main()