"""Modulo para envío de SMS automáticos"""

a__version__ = "0.83"

import base64
import copy
import os
import re
import sys
import traceback
from datetime import datetime
from string import Template

import pandas as pd
import paramiko
import phonenumbers
import sqlalchemy
from dotenv import load_dotenv
from ruamel.yaml import YAML
from smsapi.client import SmsApiComClient
from smsapi.exception import SendException
from sqlalchemy import Table, Column, String, DateTime

load_dotenv(".env")


def get_from_ftp(file: str) -> None:
    """Function to get files from the SFTP server - deletes the file after downloading"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=os.environ.get("SFTP_HOST"),
        port=int(os.environ.get("SFTP_PORT")),
        username=os.environ.get("SFTP_USER"),
        password=base64.b64decode(os.environ.get("SFTP_PASS").encode()).decode(),
    )

    with ssh.open_sftp() as sftp:
        sftp.chdir(os.environ.get("SFTP_FOLDER"))
        sftp.get(file, os.path.join(os.getcwd(), os.environ.get("ORIGIN"), file))
        sftp.remove(file)


def get_config_file() -> dict:
    """Busca el archivo de configuración 'config.yaml' en el FTP"""
    get_from_ftp("config.yaml")
    yaml = YAML(typ="safe")
    with open(
        os.path.join(os.getcwd(), os.environ.get("ORIGIN"), "config.yaml"),
        encoding="utf-8",
    ) as file:
        _config = yaml.load(file.read())
    return _config


def get_files_to_process(config: dict) -> None:
    """Downloads the files that are mentioned in the yaml config file"""
    files_to_get = [config[entry].get("filename") for entry in config]
    for _file in files_to_get:
        if _file is not None:
            get_from_ftp(_file)


def read_csv_file(csvfile: str) -> pd.DataFrame:
    """Process a csv file and returns a dataframe"""
    _recipients = pd.read_csv(csvfile, sep=";", encoding="iso8859-1")
    if _recipients.shape[1] == 1:
        _recipients = pd.read_csv(csvfile, sep=",", encoding="iso8859-1")
    return _recipients


def get_recipient_list(recipients: pd.DataFrame) -> list[dict]:
    """Devuelve un diccionario de destinatarios para envio de SMS, con el Teléfono validado
    - Comprueba que el primer número del teléfono es 6 o 7 y que la longitud es 9 digitos
    - Comprueba que phonenumbers da el teléfono como válido"""
    recipients.columns = [
        re.sub(r"[^\da-zA-Z]+", "", column).title() for column in recipients.columns
    ]
    replacement_dict = {"Telfono": "Telefono", "Mvil": "Movil"}
    for key, value in replacement_dict.items():
        recipients.columns = list(
            map(lambda x: x.replace(key, value), recipients.columns)
        )
    if (
        "Telefono" not in recipients.columns and "Movil" not in recipients.columns
    ) or "Nombre" not in recipients.columns:
        raise ValueError("Sin columna de teléfono o nombre")
    ans_list = []
    for col in ("Telefono", "Movil"):
        try:
            recipients[col] = pd.to_numeric(recipients[col], errors="coerce")
            recipients[col] = recipients[col].fillna(0).apply(int).apply(str)
            ans_list_tmp = recipients[["Nombre", col]].to_dict("split")["data"]
            ans_list.extend(ans_list_tmp)
        except KeyError:
            pass
    if len(ans_list) == 0:
        raise ValueError("No hay mensajes que enviar")
    ans_dict = {i[1]: i[0] for i in ans_list}
    _ans = []
    for _phone, _name in ans_dict.items():
        try:
            if _phone[0] in ("6", "7") and len(_phone) == 9:
                parsed_phone = phonenumbers.parse(_phone, "ES")
                _ans.append(
                    {
                        "phone": str(parsed_phone.country_code)
                        + str(parsed_phone.national_number),
                        "name": _name.title(),
                    }
                )
        except phonenumbers.NumberParseException:
            pass
    return _ans


def get_recipients(file: str) -> list[dict]:
    """Devuelve los destinatarios del CSV"""
    return get_recipient_list(read_csv_file(file))


def send_sms(_recipients: list, text_to_send: str) -> list:
    """Envía un SMS"""
    information = []
    for recipient in _recipients:
        sms_text = Template(text_to_send).substitute(nombre=recipient.get("name"))
        try:
            _info = send_smsapi(
                apikey=os.environ.get("SMS_API_KEY"),
                phonenumber=recipient.get("phone"),
                sms_message=sms_text,
            )
            information.append(_info)
        except SendException:
            print("Error controlado en número de teléfono:", recipient.get("phone"))
            traceback.print_exc()
        except TypeError:
            pass
    return information


def send_to_recipients(recipients: list, text_to_send: str, test: bool) -> list:
    """Añade un modo Test al envío de los sms"""
    jorge = {"phone": "34656764922", "name": "Jorge"}
    if test:
        rec = [jorge]
    else:
        rec = copy.deepcopy(recipients)
        rec.append(jorge)
    return send_sms(_recipients=rec, text_to_send=text_to_send)


def load_data_into_postgres(data: list[dict]) -> None:
    """Sube los logs a postgres"""
    host_port_db = os.environ.get("DATASOURCE_URL")
    username = os.environ.get("DATASOURCE_USR")
    password = os.environ.get("DATASOURCE_PWD")

    dialect = "postgresql"
    sql_driver = "psycopg"
    conn_str = f"{dialect}+{sql_driver}://{username}:{password}@{host_port_db}"
    engine = sqlalchemy.create_engine(conn_str)
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine, schema="public")

    ans_table = Table(
        "envios",
        metadata,
        Column("outcome", String),
        Column("id", String, primary_key=True),
        Column("phone", String),
        Column("on", DateTime),
        Column("message", String),
    )

    with engine.connect() as conn:
        conn.execute(ans_table.insert(), data)
        conn.commit()


def clear_text(input_text: str) -> str:
    """Quita los caracteres especiales y limita la longitud a 160 caracteres"""
    input_text = input_text.replace("á", "a")
    input_text = input_text.replace("é", "e")
    input_text = input_text.replace("í", "i")
    input_text = input_text.replace("ó", "o")
    input_text = input_text.replace("ú", "u")
    input_text = input_text.replace("º", "o")
    input_text = input_text.replace("ª", "a")
    input_text = input_text.replace("Á", "A")
    input_text = input_text.replace("É", "E")
    input_text = input_text.replace("Í", "I")
    input_text = input_text.replace("Ó", "O")
    input_text = input_text.replace("Ú", "U")
    input_text = input_text.replace("ñ", "n")
    input_text = input_text.replace("Ñ", "N")
    input_text = re.sub("[^a-zA-Z0-9!¡?¿'=()/&%$.\" ]", "", input_text)
    return input_text[0:160]


def send_smsapi(apikey: str, phonenumber: str, sms_message: str) -> dict:
    """Envía un SMS utilizando la api de SMS-API
    https://ssl.smsapi.com/react/oauth/manage
    """
    _id, error, date_sent, points, number = "", None, None, 0.0, None
    client = SmsApiComClient(access_token=apikey)

    # send single sms
    results = client.sms.send(to=phonenumber, message=clear_text(sms_message))

    for result in results:
        _id = result.id
        number = result.number
        points = result.points
        error = result.error
        date_sent = datetime.fromtimestamp(result.date_sent)

    return {
        "outcome": "OK",
        "phone": number,
        "id": _id,
        "on": date_sent,
        "message": error,
    }


if __name__ == "__main__":
    _config = {}

    TEST = os.environ.get("TEST") == "TRUE"
    if TEST:
        _info = send_smsapi(
            apikey=os.environ.get("SMS_API_KEY"),
            phonenumber="34656764922",
            sms_message=os.environ.get("TEST_MESSAGE"),
        )
        print("SMS de prueba enviado")
        sys.exit(0)

    try:
        _config = get_config_file()
        get_files_to_process(_config)
    except FileNotFoundError:
        print("Config File Not Found -> Exiting")
        sys.exit(1)

    info = []
    for key, value in _config.items():
        filename = os.path.join(
            os.getcwd(), os.environ.get("ORIGIN"), value.get("filename")
        )
        mensaje = value.get("mensaje")
        recipients = get_recipients(filename)
        if not recipients:
            print("Sin teléfonos a los que enviar")
            sys.exit(1)
        _info = send_to_recipients(recipients, mensaje, TEST)
        info.extend(_info)

        if not TEST:
            os.remove(filename)

    if not TEST:
        os.remove(os.path.join(os.getcwd(), os.environ.get("ORIGIN"), "config.yaml"))

    load_data_into_postgres(info)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -> Finished OK")
