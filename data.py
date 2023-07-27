"""utilidades de procesamiento de datos"""

import datetime
import json
import os
import time

import cmlapi
import pandas as pd
from dotenv import load_dotenv
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext

from config import config
from config.config import logger
from src.shared import blob_storage as blob
from src.shared import pi


def validate_input() -> bool:
    """Valida los datos de entrada ejecutando la suite de GE

    Returns:
        bool: Si los datos son validos o no
    """

    data_context: DataContext = DataContext(context_root_dir="tests/great_expectations")

    result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name="novedades_gidi",
        batch_request=None,
        run_name=None,
    )

    return result.success


def get_presiones() -> pd.DataFrame:
    """Obtención de las presiones para cada nuevo evento

    Returns:
        pd.DataFrame: DataFrame con las presiones obtenidas de PI
    """

    load_dotenv()

    pi_webapi = pi.PiWebapi(
        "https://swpnqntaspi23.grupo.ypf.com/piwebapi",
        "swpnqntaspi11",
        "basic",
        False,
        os.environ.get("PI_WEB_API_USER"),
        os.environ.get("PI_WEB_API_PASSWORD"),
    )

    novedades_fp = config.DATA_INPUT_DIR.joinpath("NOVEDADES_GIDI.csv")
    df_novedades = pd.read_csv(novedades_fp)

    df_presiones = pd.DataFrame(
        columns=["Evento", "Timestamp", "Value", "Value.Name", "Value.Value", "Value.IsSystem"]
    )
    security_auth = pi_webapi.call_security_method()
    for id_evento, pozo, desde, hasta in zip(
        df_novedades["ID_EVENTO"],
        df_novedades["PADRE"],
        df_novedades["INICIO_FRAC"],
        df_novedades["FIN_FRAC"],
    ):
        tag = pozo + "_PT:CABEZA.PV"
        webid = pi_webapi.generate_webid_from_path(rf"{pi_webapi.path}\{tag}")

        presiones = pi_webapi.get_recorded_data(
            security_auth=security_auth, webid=webid, start_time=desde, end_time=hasta
        )

        list_data = list(presiones[1].values())
        df_data = pd.json_normalize(list_data[0])

        df_data["Timestamp"] = pd.to_datetime(
            df_data["Timestamp"], format="%Y-%m-%d %H:%M:%S"
        ).to_numpy()
        df_data["Timestamp"] = df_data["Timestamp"] - datetime.timedelta(hours=3)
        df_data["Timestamp"] = pd.to_datetime(df_data["Timestamp"]).dt.tz_convert(None)

        df_data = df_data.reindex(
            columns=["Evento", "Timestamp", "Value", "Value.Name", "Value.Value", "Value.IsSystem"]
        )
        df_data["Evento"] = id_evento

        df_presiones = pd.concat([df_presiones, df_data], ignore_index=True)

    return df_presiones


def run_job_estados() -> pd.DataFrame:
    """Obtención de los datos de horas de paro.
    Se ejecuta un job en CDP, ya que los datos están en Teradata

    Returns:
        pd.DataFrame: DataFrame con las horas de paro obtenidas de Teradata
    """

    load_dotenv()

    api_url = os.environ.get("CML_API_URL")
    api_key = os.environ.get("CML_API_KEY")
    api_client = cmlapi.default_client(url=api_url, cml_api_key=api_key)
    projects = api_client.list_projects(search_filter=json.dumps({"name": "parent-child"}))
    project = projects.projects[0]

    project_id = project.id
    job_id = "k9yi-cww6-iw3w-buq0"
    jobrun_body = cmlapi.CreateJobRunRequest(project_id, job_id)
    job_run = api_client.create_job_run(jobrun_body, project_id, job_id)
    run_id = job_run.id

    status = ""
    while status != "ENGINE_SUCCEEDED":
        api_response = api_client.get_job_run(project_id, job_id, run_id)
        status = api_response.status
        print(status)
        time.sleep(10)

    blob.download_data_from_blob_storage(
        storage_account_key=config.STORAGE_ACCOUNT_KEY,
        container_name=config.CONTAINER_NAME,
        local_fp=config.DATA_INPUT_DIR,
        blob_name="ESTADOS.CSV",
    )
    estados_fp = config.DATA_INPUT_DIR.joinpath("ESTADOS.csv")
    df_estados = pd.read_csv(estados_fp)

    return df_estados


# if __name__ == "__main__":
#    validate_input()
