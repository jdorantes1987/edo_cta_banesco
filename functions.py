from pandas import DataFrame


# Agrega un identificador unico para la columna pasa por parámetro
def get_identificador_unicos(df, name_field) -> DataFrame:
    df["correl"] = df.groupby([name_field]).cumcount() + 1
    df["correl"] = df["correl"].astype("str")
    df["identificador"] = df[name_field] + df["correl"]
    return df
