"""utilidades de procesamiento de datos"""

import os
import urllib.parse as urlparse
from datetime import datetime
from pathlib import Path

from azure.storage.blob import BlobServiceClient


def download_data_from_blob_storage(
    storage_account_key: str, container_name: str, local_fp: Path, blob_name: str = None
) -> list[str]:
    """
    Descarga los archivos de Azure Blob Storage a una carpeta local

    Args:
        storage_account_key (str): clave de la cuenta de almacenamiento Azure Blob Storage
        container_name (str): nombre del container para los datos del proyecto
        local_fp (Path): carpeta local donde se guardarán los blobs
        blob_name (str, optional): nombre del archivo que queremos bajar.
                                   Si no le pasamos valor, bajará todos los blobs.
                                   Valor default es None.

    Returns:
        list[str]: lista de los archivos que fueron bajados
    """

    blob_service_client = BlobServiceClient.from_connection_string(storage_account_key)
    container_client = blob_service_client.get_container_client(container_name)
    files = []
    if blob_name is None:
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            if blob.size > 0 and blob.name[0:6] == "input/":
                file_name = blob.name.split("/")[1]
                files.append(file_name)
                local_file = local_fp.joinpath(file_name)
                file_content = container_client.get_blob_client(blob).download_blob().readall()
                with open(file=local_file, mode="wb") as file:
                    file.write(file_content)
    else:
        files.append(blob_name)
        local_file = local_fp.joinpath(blob_name)
        file_content = (
            container_client.get_blob_client("input/" + blob_name).download_blob().readall()
        )
        with open(file=local_file, mode="wb") as file:
            file.write(file_content)

    return files


def upload_data_to_blob_storage(
    storage_account_key: str,
    container_name: str,
    output_fp: Path,
    processed_fp: Path,
):
    """
    Carga los archivos de las carpetas locales (output y processed) en Azure Blob Storage

    Args:
        storage_account_key (str): clave de la cuenta de almacenamiento Azure Blob Storage
        container_name (str): nombre del container para los datos del proyecto
        output_fp (Path): path de la carpeta Output
        processed_fp (Path): path de la carpeta Processed
    """

    blob_service_client = BlobServiceClient.from_connection_string(storage_account_key)
    container_client = blob_service_client.get_container_client(container_name)

    lista_output = [f for f in os.listdir(output_fp) if os.path.isfile(os.path.join(output_fp, f))]
    if len(lista_output) > 0:
        for file in lista_output:
            path_origen = output_fp.joinpath(file)
            path_destino = "output/" + file

            with open(path_origen, "rb") as data:
                container_client.upload_blob(name=path_destino, data=data, overwrite=True)

            # if is_uploaded:
            os.remove(path_origen)

    lista_ant = [
        f for f in os.listdir(processed_fp) if os.path.isfile(os.path.join(processed_fp, f))
    ]
    if len(lista_ant) > 0:
        for file in lista_ant:
            path_origen = processed_fp.joinpath(file)
            path_destino = "processed/" + file

            with open(path_origen, "rb") as data:
                container_client.upload_blob(name=path_destino, data=data, overwrite=True)

            # if is_uploaded:
            os.remove(path_origen)


def upload_log_to_blob_storage(storage_account_key: str, container_name: str):
    """
    Carga los archivos de log en Azure Blob Storage

    Args:
        storage_account_key (str): clave de la cuenta de almacenamiento Azure Blob Storage
        container_name (str): nombre del container para los datos del proyecto
    """

    blob_service_client = BlobServiceClient.from_connection_string(storage_account_key)
    container_client = blob_service_client.get_container_client(container_name)

    path_origen = "logs/info.log"
    path_destino = "logs/log_" + datetime.now().strftime("%Y%m%d") + ".log"

    with open(path_origen, "rb") as data:
        container_client.upload_blob(name=path_destino, data=data, overwrite=True)


def clean_data(storage_account_key: str, container_name: str):
    """
    Elimina los blobs de la carpeta input

    Args:
        storage_account_key (str): clave de la cuenta de almacenamiento Azure Blob Storage
        container_name (str): nombre del container para los datos del proyecto
    """

    blob_service_client = BlobServiceClient.from_connection_string(storage_account_key)
    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs()
    blob_list_delete = [
        urlparse.quote(b.name.encode("utf8"))
        for b in blob_list
        if b.size > 0 and b.name[0:6] == "input/"
    ]
    if len(blob_list_delete) > 0:
        container_client.delete_blobs(*blob_list_delete)
