import pandas as pd

def limpiar_datos(df_extraido, engine_destino=None, tabla_destino=None):
    print("\n==============================================================")
    print("            PASO 3: LIMPIEZA Y VALIDACIÓN DE DATOS           ")
    print("==============================================================")

    df = df_extraido.copy()
    registros_iniciales = len(df)
    print(f"\nRegistros recibidos: {registros_iniciales}")


    print("\n--------------------------------------------------------------")
    print(" 1. Revisión de duplicados internos")
    print("--------------------------------------------------------------")

    cantidad_duplicados = df.duplicated().sum()

    if cantidad_duplicados > 0:
        print(f"\nSe encontraron {cantidad_duplicados} filas duplicadas en los datos extraídos.")
        df = df.drop_duplicates()
        print(f"Duplicados eliminados. Registros restantes: {len(df)}")
    else:
        print("No se encontraron duplicados internos.")

    print("\n--------------------------------------------------------------")
    print(" 2. Revisión contra registros ya existentes en destino")
    print("--------------------------------------------------------------")

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
                print("No se encontró PK en la tabla destino. Se detiene el proceso.")
                return None

            print(f"\nPK detectada(s) en '{tabla_destino}': {', '.join(pks_destino)}")

            pks_validas = [pk for pk in pks_destino if pk in df.columns]

            if not pks_validas:
                print(f"⚠ Los datos extraídos no corresponden a la tabla destino '{tabla_destino}'.")
                print(f"  Selecciona una tabla destino compatible con los datos extraídos.")
                return None

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

            print(f"Registros ya existentes (omitidos): {ya_existentes}")
            print(f"Registros nuevos a insertar:        {len(df_nuevo)}")

        except Exception as e:
            print(f"No se pudo comparar con la tabla destino: {e}")
            return None
    else:
        print("No se proporcionó tabla destino. Se omite esta validación.")

    print("\n==============================================================")
    print("                     RESUMEN DE LIMPIEZA                     ")
    print("==============================================================")
    print(f"  Registros originales:         {registros_iniciales}")
    print(f"  Registros tras limpieza:      {len(df)}")
    print(f"  Registros listos a insertar:  {len(df_nuevo)}")
    print("==============================================================")

    if df_nuevo.empty:
        print("\nNo hay registros nuevos para insertar.")
        return None

    print(f"\nVista previa de registros a insertar:")
    print(df_nuevo.head())

    return df_nuevo