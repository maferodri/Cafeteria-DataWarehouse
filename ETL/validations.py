# validaciones.py
import pandas as pd
from colorama import Fore

DIMENSIONES_REQUERIDAS = {
    "HECHOS_ORDEN": ["DIM_CUSTOMERS", "DIM_STORES", "DIM_ORDER_CHANNEL", 
                     "DIM_DATE", "DIM_TIME", "DIM_ORDER_DETAILS"]
}

def verificar_dimensiones(engine, tabla_dest):
    if tabla_dest not in DIMENSIONES_REQUERIDAS:
        return True

    dimensiones = DIMENSIONES_REQUERIDAS[tabla_dest]
    dims_vacias = []

    for dim in dimensiones:
        query = f"SELECT COUNT(*) FROM {dim}"
        resultado = pd.read_sql(query, engine)
        count = resultado.iloc[0, 0]
        if count == 0:
            dims_vacias.append(dim)

    if dims_vacias:
        print(f"\n{Fore.RED}No se puede insertar en '{tabla_dest}' porque las siguientes dimensiones están vacías:")
        for dim in dims_vacias:
            print(f"{Fore.RED}  - {dim}")
        print(f"{Fore.YELLOW}Por favor llena las dimensiones primero.")
        return False

    return True