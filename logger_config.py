import logging
import os
from datetime import datetime


def configurar_logger(nivel: str = "INFO", carpeta: str = "logs",
                      archivo: str = "etl_worker.log") -> logging.Logger:
    """
    Configura el logger central del ETL Worker.
    Escribe en consola y en archivo de log simultaneamente.
    """
    os.makedirs(carpeta, exist_ok=True)

    nivel_log = getattr(logging, nivel.upper(), logging.INFO)
    formato = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    fecha_hora = datetime.now().strftime("%Y%m%d")
    ruta_archivo = os.path.join(carpeta, f"{fecha_hora}_{archivo}")

    logging.basicConfig(
        level=nivel_log,
        format=formato,
        handlers=[
            logging.StreamHandler(),                          # consola
            logging.FileHandler(ruta_archivo, encoding="utf-8")  # archivo
        ]
    )

    logger = logging.getLogger("etl_worker")
    logger.info(f"Logger iniciado — nivel: {nivel} — archivo: {ruta_archivo}")
    return logger
