import pandas as pd
from colorama import Fore, Style, init

# Colores
init(autoreset=True)

def limpiar_datos(df_extraido, engine_destino=None, tabla_destino=None):
    print(f"\n{Fore.CYAN}==============================================================")
    print(f"{Fore.CYAN}            PASO 3: LIMPIEZA Y VALIDACIÓN DE DATOS            ")
    print(f"{Fore.CYAN}==============================================================")

    df = df_extraido.copy()
    registros_iniciales = len(df)
    print(f"\nRegistros recibidos: {registros_iniciales}")


    print(f"\n{Fore.CYAN}--------------------------------------------------------------")
    print(" 1. Revisión de duplicados internos")
    print(f"{Fore.CYAN}--------------------------------------------------------------")

    cantidad_duplicados = df.duplicated().sum()

    if cantidad_duplicados > 0:
        print(f"\n{Fore.YELLOW}Se encontraron {cantidad_duplicados} filas duplicadas en los datos extraídos.")
        df = df.drop_duplicates()
        print(f"Duplicados eliminados. Registros restantes: {len(df)}")
    else:
        print(f"{Fore.GREEN}No se encontraron duplicados internos.")

    print(f"\n{Fore.CYAN}--------------------------------------------------------------")
    print(" 2. Revisión contra registros ya existentes en destino")
    print(f"{Fore.CYAN}--------------------------------------------------------------")

    df_nuevo = df.copy()

    if engine_destino and tabla_destino:
        try:
            query_pk = f"""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = '{tabla_destino}'
                AND CONSTRAINT_NAME LIKE 'PK%'
            """
            pk_df = pd.read_sql(query_pk, engine_destino)
            pks_destino = pk_df['COLUMN_NAME'].tolist()

            if not pks_destino:
                print(f"{Fore.RED}No se encontró PK en la tabla destino. Se detiene el proceso.")
                return None

            print(f"\nPK detectada(s) en '{tabla_destino}': {', '.join(pks_destino)}")

            pks_validas = [pk for pk in pks_destino if pk in df.columns]

            if not pks_validas:
                print(f"{Fore.RED}Los datos extraídos no corresponden a la tabla destino '{tabla_destino}'.")
                print(f"{Fore.RED}Selecciona una tabla destino compatible con los datos extraídos.")
                return "WRONG_TABLE"

            cols_pk_str = ", ".join(pks_validas)
            df_existente_pk = pd.read_sql(f"SELECT {cols_pk_str} FROM {tabla_destino}", engine_destino)
            print(f"Registros actuales en '{tabla_destino}': {len(df_existente_pk)}")

            df_merged = df.merge(
                df_existente_pk,
                on=pks_validas,
                how='left',
                indicator=True
            )
            df_nuevo = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            ya_existentes = len(df) - len(df_nuevo)

            print(f"{Fore.YELLOW}Registros ya existentes (omitidos): {ya_existentes}")
            print(f"{Fore.GREEN}Registros nuevos a insertar:        {len(df_nuevo)}")

        except Exception as e:
            print(f"{Fore.RED}No se pudo comparar con la tabla destino: {e}")
            return None
    else:
        print(f"{Fore.YELLOW}No se proporcionó tabla destino. Se omite esta validación.")

    print(f"\n{Fore.CYAN}--------------------------------------------------------------")
    print(" 3. Resumen de la limpieza: ")
    print(f"{Fore.CYAN}--------------------------------------------------------------")
    print(f"  Registros originales:         {registros_iniciales}")
    print(f"  Registros tras limpieza:      {len(df)}")
    print(f"  {Fore.GREEN}Registros listos a insertar:  {len(df_nuevo)}")
    print(f"{Fore.CYAN}--------------------------------------------------------------")

    if df_nuevo.empty:
        print(f"\n{Fore.YELLOW}No hay registros nuevos para insertar.")
        return None

    return df_nuevo