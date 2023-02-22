import logging
import logging.config
import sys
from pathlib import Path

from rich.logging import RichHandler

# Directorios
BASE_DIR = Path(__file__).parent.parent.absolute()
CONFIG_DIR = Path(BASE_DIR, "config")
DATA_DIR = Path(BASE_DIR, "data")
DATA_INPUT_DIR = Path(DATA_DIR, "input")
DATA_OUTPUT_DIR = Path(DATA_DIR, "output")
DATA_PROCESSED_DIR = Path(DATA_DIR, "processed")
MODELS_DIR = Path(BASE_DIR, "models")
STORES_DIR = Path(BASE_DIR, "stores")
LOGS_DIR = Path(BASE_DIR, "logs")
FLAG_FILE = BASE_DIR.joinpath("fflag.txt")

# Stores
MODEL_REGISTRY = Path(STORES_DIR, "model")
BLOB_STORE = Path(STORES_DIR, "blob")

# Crear directorios
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATA_INPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)
MODEL_REGISTRY.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
BLOB_STORE.mkdir(parents=True, exist_ok=True)

# Logger
logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "minimal": {"format": "%(message)s"},
        "detailed": {
            "format": "%(levelname)s %(asctime)s [%(name)s:%(filename)s:%(funcName)s"
            ":%(lineno)d]\n%(message)s\n"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "minimal",
            "level": logging.DEBUG,
        },
        "info": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": Path(LOGS_DIR, "info.log"),
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.INFO,
        },
        "error": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": Path(LOGS_DIR, "error.log"),
            "maxBytes": 10485760,  # 1 MB
            "backupCount": 10,
            "formatter": "detailed",
            "level": logging.ERROR,
        },
    },
    "root": {
        "handlers": ["console", "info", "error"],
        "level": logging.INFO,
        "propagate": True,
    },
}
logging.config.dictConfig(logging_config)
logger = logging.getLogger()
logger.handlers[0] = RichHandler(markup=True)

# Activos
STORAGE_ACCOUNT_NAME = "teczdstacoe004"
CONTAINER_NAME = "mineralogia"
STORAGE_ACCOUNT_KEY = "DefaultEndpointsProtocol=https;AccountName=teczdstacoe004;AccountKey=5Gujy0/2hOx4v3AaBpk8N9J1vUah6IAhr/bSH7LEq/JbhDQHGEfMvilnC/dXw3DmilnCSGrd1YKEa2NbQBUMdA==;EndpointSuffix=core.windows.net"

# Configuración del modelo
OBJETIVO = "TOC"
MODELO = "model_toc_gpr_densidad.joblib"
IC_CLASS = "ic_class_intervalo_confianza.pkl"
# Este es el orden en que debe recibir las variables el modelo
MODEL_TOC_GPR_DENSIDAD = ["DEPTH", "RHOZ"]
MODEL_TOC_CUERVO = ["DEPTH", "RHOZ", "AT90", "DTCO"]
