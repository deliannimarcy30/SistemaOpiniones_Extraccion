import asyncio
import concurrent.futures
import json
import logging
import os
import time
from dotenv import load_dotenv

from extractors.csv_extractor      import CsvExtractor
from extractors.database_extractor import DatabaseExtractor
from extractors.api_extractor      import ApiExtractor
from staging.staging_writer        import StagingWriter
from logs.logger_config            import configurar_logger


load_dotenv()


BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.json")

with open(CONFIG_PATH, encoding="utf-8") as f:
    config = json.load(f)


log_cfg = config["logs"]
logger  = configurar_logger(
    nivel   = log_cfg["nivel"],
    carpeta = log_cfg["carpeta"],
    archivo = log_cfg["archivo"]
)


def extraer_csv(writer: StagingWriter) -> bool:
    cfg = config["fuentes"]["csv"]
    extractor = CsvExtractor(
        ruta               = os.getenv("CSV_PATH", "data/opiniones.csv"),
        encoding           = cfg["encoding"],
        separador          = cfg["separador"],
        columnas_requeridas= cfg["columnas_requeridas"]
    )
    df = extractor.extract()
    if extractor.validar(df):
        writer.guardar(df, "staging_encuestas")
        return True
    return False


def extraer_base_de_datos(writer: StagingWriter) -> bool:
    cfg = config["fuentes"]["base_de_datos"]
    extractor = DatabaseExtractor(
        server   = os.getenv("DB_SERVER"),
        port     = os.getenv("DB_PORT", "1433"),
        database = os.getenv("DB_NAME"),
        username = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        driver   = cfg["driver"],
        query    = cfg["query"]
    )
    df = extractor.extract()
    if extractor.validar(df):
        writer.guardar(df, "staging_resenas")
        return True
    return False


def extraer_api(writer: StagingWriter) -> bool:
    cfg = config["fuentes"]["api"]
    extractor = ApiExtractor(
        base_url       = os.getenv("API_BASE_URL"),
        endpoint       = os.getenv("API_ENDPOINT", "/comments"),
        timeout        = int(os.getenv("API_TIMEOUT", 30)),
        max_reintentos = cfg["max_reintentos"],
        limite         = cfg["limite_registros"]
    )
    df = extractor.extract()
    if extractor.validar(df):
        writer.guardar(df, "staging_comentarios")
        return True
    return False



def ejecutar_extraccion():
    
    inicio_total = time.time()
    logger.info("=" * 60)
    logger.info("  ETL WORKER — INICIO DE EXTRACCION")
    logger.info("=" * 60)

    writer  = StagingWriter(carpeta=config["staging"]["carpeta"])
    cfg_fts = config["fuentes"]

   
    tareas = []
    if cfg_fts["csv"]["activa"]:
        tareas.append(("CSV",           extraer_csv,           writer))
    if cfg_fts["base_de_datos"]["activa"]:
        tareas.append(("Base de Datos", extraer_base_de_datos, writer))
    if cfg_fts["api"]["activa"]:
        tareas.append(("API REST",      extraer_api,           writer))

    resultados = {}

    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tareas)) as executor:
        futuros = {
            executor.submit(fn, writer): nombre
            for nombre, fn, writer in tareas
        }
        for futuro in concurrent.futures.as_completed(futuros):
            nombre = futuros[futuro]
            try:
                resultados[nombre] = futuro.result()
            except Exception as e:
                logger.error(f"[{nombre}] Error no controlado: {e}")
                resultados[nombre] = False

   
    duracion_total = round(time.time() - inicio_total, 2)
    exitosas  = sum(1 for v in resultados.values() if v)
    fallidas  = sum(1 for v in resultados.values() if not v)

    logger.info("=" * 60)
    logger.info(f"  EXTRACCION FINALIZADA en {duracion_total}s")
    logger.info(f"  Exitosas: {exitosas} | Fallidas: {fallidas}")
    for fuente, ok in resultados.items():
        estado = "OK" if ok else "FALLO"
        logger.info(f"    [{estado}] {fuente}")
    logger.info("=" * 60)

    return resultados


if __name__ == "__main__":
    ejecutar_extraccion()
