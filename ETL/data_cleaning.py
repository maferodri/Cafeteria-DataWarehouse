import pandas as pd

def limpiar_datos(df_origen, engine_olap, tabla_destino, columnas_destino):
    print(f"\n --- Iniciando Limpieza para {tabla_destino} ---")

    columnas_comunes = [c for c in df_origen.columns if c in columnas_destino]

    if not columnas_comunes:
        print("Se procesaron todos los datos")
        return df_origen

    try:
        cols_str = ", ".join(columnas_comunes)
        query_check = f"SELECT {cols_str} FROM {tabla_destino}"
        df_existente = pd.read_sql(query_check, engine_olap)

        df_merge = df_origen.merge(
            df_existente,
            on=columnas_comunes,
            how='left',
            indicator=True
        )

        df_final = df_merge[df_merge['_merge'] == 'left_only'].drop(columns=['_merge'])

        print(f" Registros originales: {len(df_origen)}")
        print(f" Registros nuevos detectados: {len(df_final)}")

        return df_final
    
    except Exception as e:
        print(f"Error en la limpieza: {e}")
        return df_origen