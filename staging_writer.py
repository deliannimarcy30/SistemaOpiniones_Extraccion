import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


class StagingWriter:
   

    def __init__(self, carpeta: str = "staging"):
        self.carpeta = carpeta
        os.makedirs(carpeta, exist_ok=True)

    def guardar(self, df: pd.DataFrame, nombre: str) -> str:
       
        if df.empty:
            logger.warning(f"[Staging] DataFrame vacio, no se guarda '{nombre}'")
            return ""

        ruta = os.path.join(self.carpeta, f"{nombre}.parquet")
        try:
            df.to_parquet(ruta, index=False, engine="pyarrow")
            logger.info(f"[Staging] Guardado '{ruta}' — {len(df)} filas, {df.shape[1]} columnas")
            return ruta
        except Exception as e:
            logger.error(f"[Staging] Error al guardar '{nombre}': {e}")
            return ""

    def leer(self, nombre: str) -> pd.DataFrame:
        """Lee un archivo Parquet desde staging."""
        ruta = os.path.join(self.carpeta, f"{nombre}.parquet")
        if not os.path.exists(ruta):
            logger.error(f"[Staging] Archivo no encontrado: {ruta}")
            return pd.DataFrame()
        return pd.read_parquet(ruta, engine="pyarrow")
