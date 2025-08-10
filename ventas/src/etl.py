import pandas as pd
from pathlib import Path
from utils import get_sqlite_conn

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
RAW_FILE = DATA_DIR / "ventas.csv"
DB_FILE = DATA_DIR / "ventas.db"
PARQUET_FILE = DATA_DIR / "ventas_limpias.parquet"

def extract() -> pd.DataFrame:
    df = pd.read_csv(RAW_FILE)
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Eliminar filas con nulos críticos
    df = df.dropna(subset=["cantidad", "precio_unitario", "fecha"]).copy()
    # Tipos
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])
    df["cantidad"] = df["cantidad"].astype(int)
    df["precio_unitario"] = df["precio_unitario"].astype(float)
    # Campos derivados
    df["total"] = (df["precio_unitario"] * df["cantidad"]).round(2)
    # Normalizaciones sencillas
    df["region"] = df["region"].str.upper().str.strip()
    df["producto"] = df["producto"].str.strip().str.title()
    return df

def load(df: pd.DataFrame):
    with get_sqlite_conn(str(DB_FILE)) as conn:
        df.to_sql("ventas_limpias", conn, if_exists="replace", index=False)
    # Export opcional a parquet
    try:
        df.to_parquet(PARQUET_FILE, index=False)
    except Exception as e:
        print(f"Parquet no generado: {e}")

def main():
    print("Extract...")
    df_raw = extract()
    print(f"Filas brutas: {len(df_raw)}")
    print("Transform...")
    df_clean = transform(df_raw)
    print(f"Filas limpias: {len(df_clean)}")
    print("Load...")
    load(df_clean)
    print(f"Listo. DB: {DB_FILE.name} | Tabla: ventas_limpias")

if __name__ == "__main__":
    main()
