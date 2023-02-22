"""utilidades suplementarias"""

import shutil

from config import config
from config.config import logger


def read_fflag_status() -> str:
    """Lee el archivo de estado"""
    logger.debug("[read_fflag_status] Inicia")
    flag_file = config.FLAG_FILE

    try:
        with open(flag_file, "r", encoding="utf-8") as fflag:
            status = fflag.read()
            logger.info("[read_fflag_status] flag actual es: %s", status)

        logger.debug("[read_fflag_status] Finaliza")

        return status

    except OSError as ex:
        logger.error("Se produjo un error intentando leer el status")
        logger.error("OS error(%s): %s", ex.errno, ex.strerror)

        return "Error"


def change_fflag_status(new_status: str):
    """change_fflag_status - cambia el estado de la API.
    El estado de la API se encuentra escrito en el archivo fflag.txt
    y admite dos valores "PROCESANDO" y "STANDBY"
    Parámetros :
    - new_status: nuevo estado de la API
    """
    logger.debug("[change_fflag_status] Inicia")
    flag_file = config.FLAG_FILE

    try:
        with open(flag_file, "w", encoding="utf-8") as fflag:
            fflag.write(new_status)
            logger.info("[ejecutar_proceso] Cambia archivo fflag.txt a %s", new_status)

        logger.debug("[change_fflag_status] Finaliza")

    except OSError as ex:
        logger.error("Se produjo un error intentando escribir el status")
        logger.error("OS error(%s): %s", ex.errno, ex.strerror)


def comprimir_directorio(archivo_comprimido, directorio):
    """comprimir_directorio - crea un archivo zip con el contenido de un directorio
    Parámetros :
    - archivo_comprimido: nombre del archivo comprimido de destino
    - directorio: directorio origen a comprimir
    Retorno:
    - True si el directorio se comprimió correctamente
    - False si hubo problemas al comprimir el directorio
    """
    logger.debug("[comprimir_directorio] Inicia")
    logger.info(
        "[comprimir_directorio] \
        Comprimo directorio %s en archivo %s",
        directorio,
        archivo_comprimido,
    )
    try:
        shutil.make_archive(archivo_comprimido, "zip", directorio)
        return True

    except shutil.Error as ex:
        logger.error("[comprimir_directorio] No se pudo comprimir el directorio.")
        logger.error("Error: %s", ex.strerror)
        return False
