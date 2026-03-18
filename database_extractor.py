import logging
import time
import pandas as pd
from sqlalchemy import create_engine, text
from extractors.iextractor import IExtractor

logger = logging.getLogger(__name__)


class DatabaseExtractor(IExtractor):
    

    def __init__(self, server: str, port: str, database: str,
                 driver: str, query: str, **kwargs):
        self.query = query
        conn_str = (
            f"mssql+pyodbc://{server},{port}/{database}"
            f"?driver={driver.replace(' ', '+')}"
            f"&trusted_connection=yes"
        )
        self.engine = create_engine(conn_str, fast_executemany=True)

    def extract(self) -> pd.DataFrame:
        inicio = time.time()
        logger.info("[DB] Iniciando extraccion desde SQL Server")

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(self.query), conn)

            duracion = round(time.time() - inicio, 2)
            logger.info(f"[DB] Extraccion completada — {len(df)} filas en {duracion}s")
            return df

        except Exception as e:
            logger.error(f"[DB] Error de conexion o consulta: {e}")
            return pd.DataFrame()

    def validar(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning("[DB] DataFrame vacio")
            return False

        columnas_esperadas = [
            "IdComentario", "IdCliente", "IdProducto",
            "IdFuente", "IdFecha", "Puntaje", "Comentario"
        ]
        faltantes = [c for c in columnas_esperadas if c not in df.columns]
        if faltantes:
            logger.error(f"[DB] Columnas faltantes: {faltantes}")
            return False

        duplicados = df.duplicated(subset=["IdComentario"]).sum()
        if duplicados > 0:
            logger.warning(f"[DB] Se encontraron {duplicados} registros duplicados")

        logger.info("[DB] Validacion correcta")
        return True