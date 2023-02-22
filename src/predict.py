"""utilidades de inferencia"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

import lasio
import numpy as np
import pandas as pd


def write_las_file(file_name: Path, well: str, data: pd.DataFrame) -> None:
    """
    Guarda un dataframe como archivo .las

    Args:
        file_name (str): nombre del archivo .las
        well (str): nombre del pozo
        data (pd.DataFrame): dataframe con el contenido del archivo las
    """

    las = lasio.LASFile()
    las.well.WELL = well
    las.well.DATE = datetime.today().strftime("%d/%m/%Y")
    las.well.NULL = -999.0000
    for mnemonic in data.columns:
        las.append_curve(mnemonic, data[mnemonic])
    las.write(str(file_name), version=2, mnemonics_header=True, data_section_header="~ASCII")


def cuervo_model(df: pd.DataFrame) -> float:
    """
    Calcula TOC_Cuervo

    Args:
        df (pd.DataFrame): dataframe que contiene los features de cuervo

    Returns:
        float: valor para la predicción TOC_Cuervo
    """
    X_rhoz, X_at90, X_dtco = df["RHOZ"], df["AT90"], df["DTCO"]
    return 100 * (-0.472 + (1.12 / X_rhoz) + 0.0072 * np.log10(X_at90) + 0.00048 * X_dtco)


def predict_model(
    archivo: str,
    artifacts: Dict,
    data: pd.DataFrame,
    output_dir: Path,
    objetivo: str = "TOC",
    well: str = "Pozo_UNK",
    ejecuta_cuervo: bool = False,
    df_cuervo: pd.DataFrame = pd.DataFrame,
):
    """
    Predecir los perfiles sinteticos

    Args:
        archivo (str): archivo .las
        artifacts (Dict): diccionario con los artefactos del modelo
        output_dir (str, optional): carpeta de salida. Valor default es "output".
        objetivo (str, optional): prefijo utilizado en las columnas target. Valor default es "TOC".
        data (pd.DataFrame, optional): dataframe COE ["DEPTH", "RHOZ"]. Valor default es None.
        well (str, optional): nombre del pozo. Valor default es "Pozo_UNK".
        ejecuta_cuervo (bool, optional): si ejecutar también la predicción Cuervo. \
            Valor default es False.
        df_cuervo (pd.DataFrame, optional): dataframe Cuervo ["DEPTH", "RHOZ", "AT90", "DTCO"]. \
            Valor default es pd.DataFrame.
    """

    # nombre para guardar
    name = archivo[:-4]

    lognames = "DS_log.predict"
    loggers = logging.getLogger(lognames)

    loggers.info("COMIENZA el proceso de predicción")
    fited_model = artifacts["model"]
    ic_class_predict = artifacts["ic_class"]

    # Prediccion
    loggers.info("Ejecutando predicción del modelo TOC_COE...")
    # El predictor no necesita DEPTH
    pred_y = fited_model.predict(data.loc[:, data.columns != "DEPTH"])

    loggers.info("Ejecutando predicción del intervalo de confianza (ic_class)...")
    int_confianza = ic_class_predict.maronna_confidence_intervals(y_pred=pred_y, alpha=0.1)
    int_confianza[int_confianza < 0] = 0

    # Escribo la salida
    archivo_xls = name + "_TOC_COE.xlsx"
    path_salida_xls = output_dir.joinpath(archivo).joinpath(archivo_xls)
    loggers.info("Escribiendo Excel de salida (XLSX) %s", archivo_xls)
    df_result = pd.concat(
        [
            data,
            pd.DataFrame(pred_y, columns=[objetivo + "_COE"]),
            pd.DataFrame(
                int_confianza[["IC_INF", "IC_SUP"]].values,
                columns=[
                    objetivo + "_COE_cota_inferior",
                    objetivo + "_COE_cota_superior",
                ],
            ),
        ],
        axis=1,
    )

    df_result[df_result < 0] = -999.0000
    df_result.to_excel(path_salida_xls, index=False)

    # Escribo .las
    archivo_las = name + "_TOC_COE.las"
    path_salida_las = output_dir.joinpath(archivo).joinpath(archivo_las)
    loggers.info("Escribiendo perfil de salida (LAS) %s", archivo_las)
    write_las_file(file_name=path_salida_las, well=well, data=df_result)
    loggers.info("FINALIZA proceso predicción")

    # prediccion de cuervo
    if ejecuta_cuervo:
        loggers.info("COMIENZA el proceso de predicción del modelo TOC_Cuervo.")
        loggers.info("Ejecutando predicción del modelo TOC_Cuervo...")
        y_cuervo = cuervo_model(df_cuervo.dropna())
        df_result = pd.concat(
            [
                df_cuervo,
                pd.DataFrame(y_cuervo, columns=["TOC_Cuervo"]),
            ],
            axis=1,
        )
        # Escribo la salida excel
        archivo_xls = name + "_TOC_Cuervo.xlsx"
        path_salida_xls = output_dir.joinpath(archivo).joinpath(archivo_xls)
        loggers.info("Escribiendo Excel de salida (XLSX) %s", archivo_xls)
        df_result[df_result < 0] = 0
        df_result.to_excel(path_salida_xls, index=False)

        # Escribo salida .las
        archivo_las = name + "_TOC_Cuervo.las"
        path_salida_las = output_dir.joinpath(archivo).joinpath(archivo_las)
        loggers.info("Escribiendo perfil de salida (LAS) %s", archivo_las)
        write_las_file(file_name=path_salida_las, well=well, data=df_result)
        loggers.info("FINALIZA proceso predicción del modelo TOC_Cuervo.")
