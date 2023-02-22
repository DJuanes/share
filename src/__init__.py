"""ejecutar_proceso.py - punto de entrada del endpoint "submit":
    - Importación de librerías
    - Inicializacion de variables de configuracion
    - Inicialización del logger
    - Cambio de status a PROCESANDO
    - Obtención de parámetros de entrada enviados por consola
    - Llamado al proceso principal
"""
from config import config
from config.config import logger
from main import predict
from src import data as data_process
from src import utils

CTE_STATUS_PROCESANDO = "PROCESANDO"
CTE_STATUS_STANDBY = "STANDBY"

status = utils.read_fflag_status()
if status == CTE_STATUS_PROCESANDO:
    logger.info("La API ya se se estaba ejecutando.")

else:

    # Cambio de status de la API a PROCESANDO. El cliente monitorea este estado
    # para saber si la API sigue procesando
    utils.change_fflag_status(CTE_STATUS_PROCESANDO)

    # Copia de datos desde el Blob Storage a la carpetas local
    files = data_process.download_data_from_blob_storage(
        storage_account_key=config.STORAGE_ACCOUNT_KEY,
        container_name=config.CONTAINER_NAME,
        local_fp=config.DATA_INPUT_DIR,
        blob_name=None,
    )
    logger.info("Archivos descargados: %s", files)

    for fileName in files:
        logger.info("[__init__] archivo: %s", fileName)

        # Llamado a la función principal
        predict(fileName)

        # Comprime el directorio generado por la predicción para enviarlo al usuario
        output_path = config.DATA_OUTPUT_DIR.joinpath(fileName)
        logger.info("[__init__] comprimir_directorio: %s", output_path)
        utils.comprimir_directorio(output_path, output_path)

        # Copia de datos desde la carpeta local al Blob Storage
        data_process.upload_data_to_blob_storage(
            storage_account_key=config.STORAGE_ACCOUNT_KEY,
            container_name=config.CONTAINER_NAME,
            output_fp=config.DATA_OUTPUT_DIR,
            processed_fp=config.DATA_PROCESSED_DIR,
        )

    # Cambio de status de la API a STANDBY. El cliente monitorea este estado
    # para saber si la API terminó el proceso
    utils.change_fflag_status(CTE_STATUS_STANDBY)

    # Almacenamiento del log en el Blob Storage
    data_process.upload_log_to_blob_storage(
        storage_account_key=config.STORAGE_ACCOUNT_KEY, container_name=config.CONTAINER_NAME
    )
    data_process.clean_data(
        storage_account_key=config.STORAGE_ACCOUNT_KEY, container_name=config.CONTAINER_NAME
    )

logger.info("[__init__] Finaliza")
