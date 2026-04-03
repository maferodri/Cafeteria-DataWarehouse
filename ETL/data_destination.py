import pandas as pd 
from colorama import Fore, Style, init

# Colores
init(autoreset=True)

engine_olap = 'DW_CAFETERIA'
def seleccionar_destino(engine_olap):
    print(f"\n{Fore.CYAN}==============================================================")
    print(f"{Fore.CYAN}            PASO 2: SELECCIÓN DE TABLA DESTINO            ")
    print(f"{Fore.CYAN}==============================================================")

    try:
        query_tables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        tablas_df = pd.read_sql(query_tables, engine_olap)
        lista_tablas = tablas_df['TABLE_NAME'].tolist()

        if not lista_tablas:
            print(f"{Fore.RED}No se encontraron tablas en el Data Warehouse.")
            return None

        print(f"\n{Fore.MAGENTA}Tablas disponibles en el Data Warehouse Cafeteria (OLAP):")
        for i, tabla in enumerate(lista_tablas, 1):
            print(f"{Fore.WHITE}{i}. {tabla}")

        while True:
            seleccion = input(f"\n{Fore.CYAN}Escribe el nombre de la tabla de destino: {Style.RESET_ALL}").strip()
            
            if seleccion in lista_tablas:
                query_cols = f"SELECT TOP 0 * FROM {seleccion}"
                columnas_destino = pd.read_sql(query_cols, engine_olap).columns.tolist()
                
                print(f"\n{Fore.GREEN}Tabla '{seleccion}' seleccionada exitosamente.")
                return seleccion, columnas_destino
            else:
                print(f"{Fore.RED}Error: La tabla '{seleccion}' no existe en el destino. Intente de nuevo.")
                
    except Exception as e:
        print(f"{Fore.RED}Error al conectar con el Data Warehouse: {e}")
        return None, None