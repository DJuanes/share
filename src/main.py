"""Workflow principal"""

import logging
import os
import pickle
import sys
from typing import Dict

import joblib
import lasio
import pandas as pd

import ic_class
import predict as pred
from config import config
from config.config import logger


def cargo_las(file: str) -> pd.DataFrame:
    """
    Genera un dataframe a partir de un archivo con formato las

    Args:
        file (str): archivo .las a cargar en un dataframe

    Returns:
        pd.DataFrame: dataframe con el contenido del .las
    """

    df_out = pd.DataFrame()

    try:
        las = lasio.read(file)
        df_out = las.df()
        df_out.reset_index(level=None, drop=False, inplace=True, col_level=0, col_fill="")
        # contenido de WELL en el header del .las
        df_out["WELL"] = las.well.WELL.value

        if "DEPTH:1" in df_out.columns:
            # Si está DEPTH:1 estará también DEPTH:2
            df_out.drop(["DEPTH:2"], axis=1, inplace=True)
            df_out.rename({"DEPTH:1": "DEPTH"}, axis=1, inplace=True)

    except lasio.las.exceptions.LASHeaderError as e:
        raise ValueError(
            "Error durante la lectura de los datos del encabezado del archivo LAS."
        ) from e

    except lasio.las.exceptions.LASDataError as e:
        raise ValueError("Error durante la lectura de datos numéricos del archivo LAS.") from e

    except lasio.las.exceptions.LASUnknownUnitError as e:
        raise ValueError("Error de unidad desconocida en archivo LAS.") from e

    return df_out


def load_artifacts() -> Dict:
    """
    Cargar artefactos para la predicción.

    Returns:
        Dict: artefactos de ejecución.
    """

    artifacts_dir = config.MODELS_DIR
    logger.info("artifacts dir: %s", artifacts_dir)
    model_fp = artifacts_dir.joinpath(config.MODELO)
    ic_class_fp = artifacts_dir.joinpath(config.IC_CLASS)
    model = joblib.load(model_fp)
    logger.info("Carga modelo %s", config.MODELO)
    with open(ic_class_fp, "rb") as f:
        ic_class_predict: ic_class = pickle.load(f)
    logger.info("Carga modelo ic_class %s", config.IC_CLASS)

    return {
        "model": model,
        "ic_class": ic_class_predict,
    }


def inicializar_logger(logfile: str) -> logging.Logger:
    """
    Inicializa el logging interno de DS

    Args:
        logfile (str): nombre del archivo de log

    Returns:
        logging.Logger: objeto logger
    """

    logname = "DS_log"
    loglevel = logging.INFO
    formatter = logging.Formatter("%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s")
    ds_logger = logging.getLogger(logname)
    ds_logger.setLevel(loglevel)
    handler = logging.FileHandler(logfile)
    handler.setLevel(loglevel)
    handler.setFormatter(formatter)
    ds_logger.addHandler(handler)

    return ds_logger


def finalizar_logger(ds_logger: logging.Logger) -> None:
    """
    Cierra todos los handlers del logger

    Args:
        ds_logger (logging.Logger): objeto logger
    """
    if ds_logger is None:
        return
    for handler in list(ds_logger.handlers):
        handler.close()
        ds_logger.removeHandler(handler)


def predict(archivo: str):
    """
    Ejecuta la predicción

    Args:
        archivo (str): nombre del archivo
    """

    artifacts = load_artifacts()

    os.makedirs(config.DATA_OUTPUT_DIR.joinpath(archivo), exist_ok=True)

    logname = archivo[:-4] + ".log"
    logfile = config.DATA_OUTPUT_DIR.joinpath(archivo).joinpath(logname)
    ds_logger = inicializar_logger(logfile=logfile)
    ds_logger.info("INICIA PROCESO: %s", archivo)

    ds_logger.info("Lectura de datos desde el perfil %s", archivo)
    input_fp = config.DATA_INPUT_DIR.joinpath(archivo)
    try:
        data = cargo_las(input_fp)
    except ValueError as e:
        ds_logger.critical("No se puede abrir el perfil %s", archivo)
        ds_logger.critical(e)
        ds_logger.info("No se puede ejecutar los modelos de predicción de TOC.")
        ds_logger.info("Moviendo archivo de la carpeta input a processed...")
        output_fp = config.DATA_PROCESSED_DIR.joinpath("/ERROR_LECTURA_" + archivo)
        os.rename(input_fp, output_fp)
        ds_logger.info("FIN PROCESO: %s", archivo)
        sys.exit(1)

    well = data["WELL"][0]
    # Predice model1?
    ok_model1 = set(config.MODEL_TOC_GPR_DENSIDAD).issubset(set(data.columns))
    ds_logger.info(
        "¿Están las columnas de profundidad y densidad (DEPTH, RHOZ)?: %s", str(ok_model1)
    )

    # Predice modelo Cuervo?
    ok_model_cuervo = set(config.MODEL_TOC_CUERVO).issubset(set(data.columns))
    ds_logger.info(
        "¿Están la columna de densidad, resistividad y sónico (DEPTH, RHOZ, AT90 y DTCO)?: %s",
        str(ok_model_cuervo),
    )

    # WARNING por rangos fuera de lo conocido por el modelo

    df_coe = data[config.MODEL_TOC_GPR_DENSIDAD]
    df_coe = df_coe[df_coe > 0].dropna().reset_index(drop=True)
    if ok_model1 & ok_model_cuervo:
        df_cuervo = data[config.MODEL_TOC_CUERVO]
        df_cuervo = df_cuervo[df_cuervo > 0].dropna().reset_index(drop=True)

        pred.predict_model(
            archivo=archivo,
            artifacts=artifacts,
            output_dir=config.DATA_OUTPUT_DIR,
            objetivo=config.OBJETIVO,
            data=df_coe,
            well=well,
            ejecuta_cuervo=True,
            df_cuervo=df_cuervo,
        )

    elif ok_model1:
        pred.predict_model(
            archivo=archivo,
            artifacts=artifacts,
            output_dir=config.DATA_OUTPUT_DIR,
            objetivo=config.OBJETIVO,
            data=df_coe,
            well=well,
            ejecuta_cuervo=False,
            df_cuervo=pd.DataFrame(),
        )
    else:
        ds_logger.critical(
            "No se encuentran las columnas requeridas para la predicción de los modelos."
        )
        ds_logger.info("No se puede ejecutar los modelos de predicción de TOC.")
        ds_logger.info("Moviendo archivo de la carpeta input a processed...")
        output_fp = config.DATA_PROCESSED_DIR.joinpath("/ERROR_LECTURA_" + archivo)
        os.rename(input_fp, output_fp)
        ds_logger.info("FIN PROCESO: %s", archivo)

        return

    ds_logger.info("Moviendo archivo de la carpeta input a processed...")
    output_fp = config.DATA_PROCESSED_DIR.joinpath(archivo)
    os.rename(input_fp, output_fp)

    ds_logger.info("FIN PROCESO: %s", archivo)
    finalizar_logger(ds_logger=ds_logger)
