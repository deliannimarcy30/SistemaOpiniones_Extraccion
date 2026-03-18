import logging
import time
import pandas as pd
from extractors.iextractor import IExtractor

logger = logging.getLogger(__name__)


class CsvExtractor(IExtractor):
    

    def __init__(self, ruta: str, encoding: str = "utf-8", separador: str = ",",
                 columnas_requeridas: list = None):
        self.ruta = ruta
        self.encoding = encoding
        self.separador = separador
        self.columnas_requeridas = columnas_requeridas or []

    def extract(self) -> pd.DataFrame:
        inicio = time.time()
        logger.info(f"[CSV] Iniciando extraccion desde: {self.ruta}")

        try:
            df = pd.read_csv(
                self.ruta,
                encoding=self.encoding,
                sep=self.separador,
                parse_dates=["Fecha"]
            )

            duracion = round(time.time() - inicio, 2)
            logger.info(f"[CSV] Extraccion completada — {len(df)} filas en {duracion}s")
            return df

        except FileNotFoundError:
            logger.error(f"[CSV] Archivo no encontrado: {self.ruta}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"[CSV] Error inesperado: {e}")
            return pd.DataFrame()

    def validar(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning("[CSV] DataFrame vacio, no hay datos para validar")
            return False

        faltantes = [c for c in self.columnas_requeridas if c not in df.columns]
        if faltantes:
            logger.error(f"[CSV] Columnas faltantes: {faltantes}")
            return False

        nulos = df[self.columnas_requeridas].isnull().sum().sum()
        if nulos > 0:
            logger.warning(f"[CSV] Se encontraron {nulos} valores nulos")

        logger.info("[CSV] Validacion correcta")
        return True
