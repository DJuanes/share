"""API del modelo predictivo"""

import json
import os
import subprocess
import time
import traceback
import uuid

from flask import Flask, request

PROYECTO = "Mineralogia"
flag_file = f"""/{PROYECTO}/fflag.txt"""
pid_file = f"""/{PROYECTO}/pidfile.txt"""

COMANDO = "python3"
pScriptPython = f"""/{PROYECTO}/src/__init__.py"""

cur_dir = os.getcwd()
log_dir = os.path.join(cur_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

api = Flask(__name__)


@api.route("/")
def init():
    """endpoint (/) - Descripción de la api
    Retorno :
    - JSON con la descripción
    """
    return json.dumps(
        f"""API de control de {PROYECTO}: Para ejecutarla usar endpoint /submit, , \
        para verificar status usar /check"""
    )


@api.route("/submit")
def submit():
    """endpoint (/submit) - Ejecución del proceso que ejecuta el modelo.
    Chequea que no haya otro proceso en ejecución. En ese caso, inicia la ejecución.
    Retorna "OK" como status si no hay errores en el submit.
    Parámetros:
    - ambiente: DEV, TEST o PRD (para que el Blob Storage apunte al ambiente correspondiente)
    - fecha_ejecucion: Opcional - se utiliza para asegurar que los datos de entrada
    son capturados desde el mismo momento para todos los pozos.
    Retorno :
    - JSON con el estado del submit y el timestamp
    """
    ambiente = request.args.get("ambiente")
    if ambiente == "":
        ambiente = "DEV"  # default

    print("API Ambiente: ", ambiente)
    # seteo la variable de ambiente para que la usen los procesos posteriores
    os.environ["AMBIENTE"] = ambiente

    out = ""
    hora = ""
    status = ""

    try:
        with open(flag_file, "r", encoding="utf-8") as fflag:
            status = fflag.read()
            hora = time.ctime(os.path.getmtime(flag_file))
            if status == "PROCESANDO":
                out = f"""API de {PROYECTO} ya se estaba ejecutando. \
                    Consultar status utilizando /check"""
    except OSError:
        out = "Error. No se pudo hacer submit()"
        print(f"""Error : {traceback.format_exc()}""")
        out = traceback.format_exc()

    # si out está vacío: no hubo errores y la API no se estaba ejecutando, continuo con el submit
    if out == "":
        _id = uuid.uuid4()

        log_file = "{}.log".format(id)
        open(os.path.join(log_dir, log_file), "w", encoding="utf-8")
        print("Ejecutar: ", COMANDO, " ", pScriptPython)
        pid = subprocess.Popen([COMANDO, pScriptPython], cwd=f"""/{PROYECTO}/src/ds""").pid
        print("PID: ", pid)

        with open(pid_file, "w", encoding="utf-8") as fpid:
            fpid.write(str(pid))
        out = "OK"
        # tomo el horario de modificación del archivo pid_file (horario en que se disparó la API)
        hora = time.ctime(os.path.getmtime(pid_file))

    return json.dumps({"status": "".join(out), "hora": "".join(hora)})


@api.route("/check")
def check():
    """endpoint (/check) - Chequeo del status de la API
    Devuelve el status que esta guardado en el archivo flag_file
    Retorno :
    - JSON con el estado de la API y el timestamp
    """
    out = "Ha ocurrido un error en el check()"
    hora = ""
    try:
        with open(flag_file, "r", encoding="utf-8") as fflag:
            out = fflag.read()
            hora = time.ctime(os.path.getmtime(flag_file))
    except OSError:
        pass

    return json.dumps({"status": "".join(out), "hora": "".join(hora)})


@api.route("/kill")
def kill():
    """endpoint (/check) - Detiene el proceso que se está ejecutando y
    escribe el status de la api en flag_file en "STANDBY"
    Retorno :
    - JSON con el estado de la API y el timestamp
    """
    out = ""
    hora = time.ctime(os.path.getmtime(flag_file))
    try:
        with open(pid_file, "r", encoding="utf-8") as fpid:
            pid = fpid.read()
        os.kill(int(pid), 9)
        out = "KILLED"
        try:
            with open(flag_file, "w", encoding="utf-8") as fflag:
                fflag.write("STANDBY")
        except OSError:
            out = "Error. Se pudo hacer el kill pero no se actualizo fflag"
    except OSError:
        out = " Error. No se pudo hacer el kill: " + str(pid)

    return json.dumps({"status": "".join(out), "hora": "".join(hora)})


@api.route("/getlogger")
def getlogger():
    """endpoint (/getlogger) - Permite ver el logueo que se está generando
    Retorno :
    - JSON con el contenido del log
    """
    try:
        with open(f"""/{PROYECTO}/logs/info.log""", encoding="utf-8") as f:
            contents = f.read()
            print(contents)
            return json.dumps({"logger": "".join(contents)})
    except OSError:
        contents = "Error. No se pudo abrir el archivo de log"
        return json.dumps({"logger": "".join(contents), "error": "".join(traceback.format_exc())})


if __name__ == "__main__":
    api.run(debug=True, host="0.0.0.0", port="443")
