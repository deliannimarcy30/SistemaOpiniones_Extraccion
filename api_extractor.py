import logging
import time
import asyncio
import httpx
import pandas as pd
from extractors.iextractor import IExtractor

logger = logging.getLogger(__name__)


class ApiExtractor(IExtractor):
    

    def __init__(self, base_url: str, endpoint: str,
                 timeout: int = 30, max_reintentos: int = 3,
                 limite: int = 500):
        self.url = f"{base_url}{endpoint}"
        self.timeout = timeout
        self.max_reintentos = max_reintentos
        self.limite = limite

    def extract(self) -> pd.DataFrame:
        return asyncio.run(self._extract_async())

    async def _extract_async(self) -> pd.DataFrame:
        inicio = time.time()
        logger.info(f"[API] Iniciando extraccion desde: {self.url}")

        for intento in range(1, self.max_reintentos + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(
                        self.url,
                        params={"_limit": self.limite}
                    )
                    response.raise_for_status()

                    datos = response.json()
                    df = pd.DataFrame(datos)

                    # Renombrar columnas para alinear con el modelo del DW
                    df = df.rename(columns={
                        "id":     "IdComentario",
                        "postId": "IdProducto",
                        "name":   "NombreAutor",
                        "email":  "EmailAutor",
                        "body":   "Comentario"
                    })

                    duracion = round(time.time() - inicio, 2)
                    logger.info(f"[API] Extraccion completada — {len(df)} registros en {duracion}s")
                    return df

            except httpx.HTTPStatusError as e:
                logger.warning(f"[API] Intento {intento}/{self.max_reintentos} — HTTP {e.response.status_code}")
            except httpx.RequestError as e:
                logger.warning(f"[API] Intento {intento}/{self.max_reintentos} — Error de red: {e}")

            if intento < self.max_reintentos:
                await asyncio.sleep(2 * intento)  # backoff exponencial

        logger.error(f"[API] Fallo tras {self.max_reintentos} intentos")
        return pd.DataFrame()

    def validar(self, df: pd.DataFrame) -> bool:
        if df.empty:
            logger.warning("[API] DataFrame vacio")
            return False

        columnas_esperadas = ["IdComentario", "IdProducto", "Comentario"]
        faltantes = [c for c in columnas_esperadas if c not in df.columns]
        if faltantes:
            logger.error(f"[API] Columnas faltantes: {faltantes}")
            return False

        nulos_comentario = df["Comentario"].isnull().sum()
        if nulos_comentario > 0:
            logger.warning(f"[API] {nulos_comentario} comentarios nulos")

        logger.info("[API] Validacion correcta")
        return True
