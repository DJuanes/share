"""Módulo de funciones para interactuar con PI"""
import base64
import json
import logging as log
import traceback
from typing import Dict
import datetime

import pandas as pd
import requests
import urllib3
from requests.auth import HTTPBasicAuth

cfgs: Dict[str, str] = {}


class PiWebapi:
    """
    Clase para encapsular las llamadas a la API de PI
    """

    def __init__(
        self,
        webapi_url: str,
        path: str,
        webapi_security_method: str,
        verify_ssl: bool,
        webapi_user: str,
        webapi_password: str,
    ):
        self._webapi_url = webapi_url
        self._path = path
        self._webapi_security_method = webapi_security_method
        self._verify_ssl = verify_ssl
        self._webapi_user = webapi_user
        self._webapi_password = webapi_password

        urllib3.disable_warnings()

    @property
    def web_api_url(self) -> str:
        """
        Returns:
            str: URL de la PI WebAPI
        """
        return self._webapi_url

    @web_api_url.setter
    def web_api_url(self, value):
        self._webapi_url = value

    @property
    def path(self) -> str:
        """
        Returns:
            str: Path de la PI WebAPI
        """
        return self._path

    @path.setter
    def path(self, value):
        self._path = value

    @property
    def webapi_security_method(self) -> str:
        """
        Returns:
            str: Método de autenticación
        """
        return self._webapi_security_method

    @webapi_security_method.setter
    def webapi_security_method(self, value):
        self._webapi_security_method = value

    @property
    def verify_ssl(self) -> bool:
        """
        Returns:
            bool: Verificación para conexión segura
        """
        return self._verify_ssl

    @verify_ssl.setter
    def verify_ssl(self, value):
        self._verify_ssl = value

    @property
    def webapi_user(self) -> str:
        """
        Returns:
            str: Usuario para conectarse a la PI WebAPI
        """
        return self._webapi_user

    @webapi_user.setter
    def webapi_user(self, value):
        self._webapi_user = value

    @property
    def webapi_password(self) -> str:
        """
        Returns:
            str: Password para conectarse a la PI WebAPI
        """
        return self._webapi_password

    @webapi_password.setter
    def webapi_password(self, value):
        self._webapi_password = value

    def call_security_method(self):
        """Método de seguridad de llamada de la API
        Propiedades:
        webApiSecurityMethod string: Método de seguridad a usar: basic o kerberos
        webApiUser string: Nombre de la credencial del usuario
        webApiPassword string: Contraseña de la credencial del usuario
        """

        p_logger = log.getLogger(cfgs["logger_name"])
        p_logger.debug("[callSecurityMethod] Inicia")

        security_auth = None

        try:
            if self.webapi_security_method.lower() == "basic":
                security_auth = HTTPBasicAuth(self.webapi_user, self.webapi_password)
            else:
                p_logger.warning(
                    "El metodo de autenticación Kerberos "
                    "no está admitido en entornos Linux"
                )
                # securityAuth = HTTPKerberosAuth(mutual_authentication='REQUIRED',
                #                                sanitize_mutual_error_response=False)

            p_logger.debug("[callSecurityMethod] Finaliza")

        except requests.RequestException as e:
            p_logger.error("Se produjo un error al tratar de autenticar: %s.", str(e))
            p_logger.error("Error: %s", traceback.format_exc())

        return security_auth

    def generate_webid_from_path(self, full_path):
        """Genera el WebId encodificando el Path completo (Path + Tag) pasado como parámetro
        @param full_path string: Nombre de Path + Tag a convertir
        """

        # p_logger = log.getLogger(cfgs["logger_name"])
        # p_logger.debug("[generateWebIdFromPath] Inicia")

        webid = ""
        fullpath_string = full_path.upper()
        fullpath_bytes = fullpath_string.encode("utf-8")

        webid = base64.b64encode(fullpath_bytes)
        webid = webid.decode("utf-8")
        webid = "P1AbE" + webid
        # P1AbE = header interno. https://docs.osisoft.com/bundle/
        # pi-web-api-reference/page/help/topics/webid-encoded-datatypes.html

        # p_logger.debug("[generateWebIdFromPath] Finaliza")

        return webid

    def check_url(self, url: str) -> str:
        """
        Función para verificar si tenemos llegado a una determinada URL

        Args:
            url (str): La URL a chequear

        Raises:
            SystemExit: Si requests.get genera un error lanzamos la excepción

        Returns:
            str: resultado del chequeo de URL
        """
        p_logger = log.getLogger(cfgs["logger_name"])
        p_logger.debug("[check_url] Inicia")

        check = ""
        try:
            get = requests.get(url, verify=False, timeout=30)
            if get.status_code == 200:
                check = f"{url}: is reachable"
            else:
                check = f"{url}: is Not reachable, status_code: {get.status_code}"

            p_logger.debug("[check_url] Finaliza")

        except requests.exceptions.RequestException as e:
            raise SystemExit(f"{url}: is Not reachable \nErr: {e}") from e

        return check

    def get_summary_data(
        self,
        security_auth,
        webid,
        start_time="-1d",
        end_time="*",
        summary_type="Average",
        duration="5m",
    ):
        """Devuelve un diccionario con una serie de tiempo para el WebId pasado como parámetro
        @param security_auth: Referencia securityAuth
        @param webid string: ID del Stream
        @param start_time string: Inicio del Stream (default -1d, es decir,
                                de la fecha actual, un día para atras)
        @param end_time string: Fin del Stream (default *, es decir, hasta el último valor)
        @param summary_type string: Clase de agregación (default es Average)
        @param duration string: Duración de cada intervalo (default es 5m, es decir,
                                cada 5 minutos)
        Propiedades:
        webApiUrl string: URL de PI Web API
        verifySSL bool: Si se realizará la verificación del certificado

        Ejemplo:
        https://swpnqntaspi23.grupo.ypf.com/piwebapi/streams/I1DPaYSBql4duEeKhGnk4sJJsw044AAA/summary?
            summaryType=Average&startTime=%222020-12-17%2012:49:00.000%22&
            endTime=%222020-12-17%2015:40:00.000%22&summaryDuration=5m&
            selectedfields=items.value.timestamp;items.value.value
        """

        p_logger = log.getLogger(cfgs["logger_name"])
        p_logger.debug("[getSummaryData] Inicia")

        data = ""
        status = 0

        proxies = {
            "http": "http://proxy-ypf.grupo.ypf.com",
            "https": "http://proxy-ypf.grupo.ypf.com",
        }

        try:
            #  armo la URL y obtengo los datos del Stream para los parámetros especificados
            request_url = (
                f"{self.web_api_url}/streams/{webid}/summary?summaryType={summary_type}&"
                f"startTime=%22{start_time}%22&endTime=%22{end_time}%22&summaryDuration={duration}&"
                "selectedfields=items.value.timestamp;items.value.value"
            )

            p_logger.debug("request_url: %s", request_url)

            # respuesta = self.check_url(request_url)
            # p_logger.info(respuesta)

            # Leer el conjunto de valores
            respuesta = requests.get(
                request_url,
                auth=security_auth,
                verify=self.verify_ssl,
                timeout=60,
                proxies=proxies,
            )
            status = respuesta.status_code

            if status == 200:
                data = json.loads(respuesta.text)
            else:
                p_logger.error(
                    "Se produjo un error al tratar de obtener los datos de PI. "
                    "Error: %s",
                    respuesta,
                )

            p_logger.debug("[getSummaryData] Finaliza")

        except requests.RequestException as e:
            p_logger.error(
                "Se produjo un error al tratar de obtener los datos de PI: %s.", str(e)
            )
            p_logger.error("Error: %s", traceback.format_exc())

        return [status, data]


    def get_recorded_data(
        self,
        security_auth,
        webid,
        start_time="-1d",
        end_time="*",
    ):
        """Devuelve un diccionario con una serie de tiempo para el WebId pasado como parámetro
        @param security_auth: Referencia securityAuth
        @param webid string: ID del Stream
        @param start_time string: Inicio del Stream (default -1d, es decir,
                                de la fecha actual, un día para atras)
        @param end_time string: Fin del Stream (default *, es decir, hasta el último valor)
        Propiedades:
        webApiUrl string: URL de PI Web API
        verifySSL bool: Si se realizará la verificación del certificado

        Ejemplo:
        https://swpnqntaspi23.grupo.ypf.com/piwebapi/streams/I1DPaYSBql4duEeKhGnk4sJJsw044AAA/summary?
            summaryType=Average&startTime=%222020-12-17%2012:49:00.000%22&
            endTime=%222020-12-17%2015:40:00.000%22&summaryDuration=5m&
            selectedfields=items.value.timestamp;items.value.value
        """

        p_logger = log.getLogger(cfgs["logger_name"])
        p_logger.debug("[get_recorded_data] Inicia")

        data = ""
        status = 0

        proxies = {
            "http": "http://proxy-ypf.grupo.ypf.com",
            "https": "http://proxy-ypf.grupo.ypf.com",
        }

        try:
            #  armo la URL y obtengo los datos del Stream para los parámetros especificados
            request_url = (
                f"{self.web_api_url}/streams/{webid}/recorded?startTime=%22{start_time}%22&endTime=%22{end_time}%22&"
                "maxCount=10000&selectedfields=items.timestamp;items.value"
            )

            p_logger.debug("request_url: %s", request_url)

            # Leer el conjunto de valores
            respuesta = requests.get(
                request_url,
                auth=security_auth,
                verify=self.verify_ssl,
                timeout=60,
                proxies=proxies,
            )
            status = respuesta.status_code

            if status == 200:
                data = json.loads(respuesta.text)
            else:
                p_logger.error(
                    "Se produjo un error al tratar de obtener los datos de PI. "
                    "Error: %s",
                    respuesta,
                )

            p_logger.debug("[get_recorded_data] Finaliza")

        except requests.RequestException as e:
            p_logger.error(
                "Se produjo un error al tratar de obtener los datos de PI: %s.", str(e)
            )
            p_logger.error("Error: %s", traceback.format_exc())

        return [status, data]


if __name__ == "__main__":
    pi_webapi_user = "YS01480"
    pi_webapi_password = "2gY7akMz"
    pi_webapi_security_method = "basic"
    pi_verify_ssl = False
    pi_webapi_url = "https://swpnqntaspi23.grupo.ypf.com/piwebapi"
    pi_path = "swpnqntaspi11"

    pi_webapi = PiWebapi(
        pi_webapi_url,
        pi_path,
        pi_webapi_security_method,
        pi_verify_ssl,
        pi_webapi_user,
        pi_webapi_password,
    )

    tag = "YPF.Nq.LACh-95(h)_PT:CABEZA.PV"
    inicio = "2022-03-20 00:00"
    fin = "2022-04-16 23:59"

    webid = pi_webapi.generate_webid_from_path(rf"{pi_webapi.path}\{tag}")
    cfgs["logger_name"] = "pi_log"
    p_logger = log.getLogger(cfgs["logger_name"])
    p_logger.setLevel(log.DEBUG)
    p_logger.addHandler(log.FileHandler("pi_log.log"))
    p_logger.debug("Path: %s, tag: %s, webid: %s", pi_webapi.path, tag, webid)

    security_auth = pi_webapi.call_security_method()
    presiones = pi_webapi.get_recorded_data(
        security_auth=security_auth, webid=webid, start_time=inicio, end_time=fin
    )
    list_data = list(presiones[1].values())
    data = list_data[0]
    df_data = pd.json_normalize(data)

    list_from_str_to_datetime = [
        "Timestamp",
    ]
    for features in list_from_str_to_datetime:
        df_data[features] = pd.to_datetime(
            df_data[features], format="%Y-%m-%d %H:%M:%S"
        ).to_numpy()
    df_data["Timestamp"] = df_data["Timestamp"] - datetime.timedelta(hours=3)
    df_data["Timestamp"] = pd.to_datetime(df_data["Timestamp"]).dt.tz_convert(
        None
    )

    archivo = "YPF.Nq.LACh-95(h)_PT_CABEZA.PV_recorded.csv"
    df_data.to_csv(archivo, index=False)
    p_logger.debug(df_data.head())
