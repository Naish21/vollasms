"""Modulo para envío de SMS automáticos"""

__version__ = "0.6"

import base64
import copy
import os
import re
import sys
from datetime import datetime
from random import randint
from string import Template
from time import sleep

import pandas as pd
import paramiko
import phonenumbers
import sqlalchemy
from dotenv import load_dotenv
from ruamel.yaml import YAML
from sqlalchemy import Table, Column, String, DateTime

from libs.volla import Volla

load_dotenv('.env')


class WrongFileException(Exception):
    """El archivo no tiene las columnas necesarias"""


def get_from_ftp(file: str) -> None:
    """Function to get files from the SFTP server - deletes the file after downloading"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=os.environ.get('SFTP_HOST'),
        port=int(os.environ.get('SFTP_PORT')),
        username=os.environ.get('SFTP_USER'),
        password=base64.b64decode(os.environ.get('SFTP_PASS').encode()).decode(),
    )

    with ssh.open_sftp() as sftp:
        sftp.chdir(os.environ.get('SFTP_FOLDER'))
        sftp.get(file, os.path.join(os.getcwd(), os.environ.get('ORIGIN'), file))
        sftp.remove(file)


def get_config_file() -> dict:
    """Busca el archivo de configuración 'config.yaml' en el FTP"""
    get_from_ftp('config.yaml')
    yaml = YAML(typ='safe')
    with open(os.path.join(os.getcwd(), os.environ.get('ORIGIN'), 'config.yaml'), encoding='utf-8') as file:
        _config = yaml.load(file.read())
    return _config


def get_files_to_process(config: dict) -> None:
    """Downloads the files that are mentioned in the yaml config file"""
    files_to_get = [config[entry].get('filename') for entry in config]
    for _file in files_to_get:
        if _file is not None:
            get_from_ftp(_file)


def read_csv_file(csvfile: str) -> pd.DataFrame:
    """Process a csv file and returns a dataframe"""
    _recipients = pd.read_csv(csvfile, sep=';', encoding='iso8859-1')
    if _recipients.shape[1] == 1:
        _recipients = pd.read_csv(csvfile, sep=',', encoding='iso8859-1')
    return _recipients


def get_recipient_list(recipients: pd.DataFrame) -> list[dict]:
    """Devuelve un diccionario de destinatarios para envio de SMS, con el Teléfono validado
    - Comprueba que el primer número del teléfono es 6 o 7 y que la longitud es 9 digitos
    - Comprueba que phonenumbers da el teléfono como válido"""
    recipients.columns = [re.sub(r"[^0-9a-zA-Z]+", "", column).title() for column in recipients.columns]
    replacement_dict = {'Telfono': 'Telefono', 'Mvil': 'Movil'}
    for key, value in replacement_dict.items():
        recipients.columns = list(map(lambda x: x.replace(key, value), recipients.columns))
    if ('Telefono' not in recipients.columns and 'Movil' not in recipients.columns) or 'Nombre' not in recipients.columns:
        raise ValueError('Sin columna de teléfono o nombre')
    ans_list = []
    for col in ('Telefono', 'Movil'):
        try:
            recipients[col] = pd.to_numeric(recipients[col], errors='coerce')
            recipients[col] = recipients[col].fillna(0).apply(int).apply(str)
            ans_list_tmp = recipients[['Nombre', col]].to_dict('split')['data']
            ans_list.extend(ans_list_tmp)
        except KeyError:
            pass
    if len(ans_list) == 0:
        raise ValueError('No hay mensajes que enviar')
    ans_dict = {i[1]: i[0] for i in ans_list}
    _ans = []
    for _phone, _name in ans_dict.items():
        try:
            if _phone[0] in ('6', '7') and len(_phone) == 9:
                phonenumbers.parse(_phone, "ES")
                _ans.append({'phone': _phone, 'name': _name.title()})
        except phonenumbers.NumberParseException:
            pass
    return _ans


def get_recipients(file: str) -> list[dict]:
    """Devuelve los destinatarios del CSV"""
    return get_recipient_list(read_csv_file(file))


def send_sms(volla: Volla, recipients: list, text_to_send: str) -> list:
    """Envía un SMS"""
    information = []
    for recipient in recipients:
        sms_text = Template(text_to_send).substitute(nombre=recipient.get('name'))
        _info = volla.send_sms(recipient.get('phone'), sms_text)
        information.append(_info)
        sleep(randint(1, 5))
    return information


def send_to_recipients(volla: Volla, recipients: list, text_to_send: str, test: bool) -> list:
    """Añade un modo Test al envío de los sms"""
    rec = copy.deepcopy(recipients)
    jorge = {'phone': '656764922', 'name': 'Jorge'}
    rec.append(jorge)
    if test:
        rec = [jorge]
    return send_sms(volla, recipients=rec, text_to_send=text_to_send)


def load_data_into_postgres(data: list[dict]) -> None:
    """Sube los logs a postgres"""
    host_port_db = os.environ.get('DATASOURCE_URL')
    username = os.environ.get('DATASOURCE_USR')
    password = os.environ.get('DATASOURCE_PWD')

    dialect = 'postgresql'
    sql_driver = 'psycopg'
    conn_str = f"{dialect}+{sql_driver}://{username}:{password}@{host_port_db}"
    engine = sqlalchemy.create_engine(conn_str)
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=engine, schema='public')

    ans_table = Table(
        'envios', metadata,
        Column('outcome', String),
        Column('id', String, primary_key=True),
        Column('phone', String),
        Column('on', DateTime),
        Column('message', String)
    )

    with engine.connect() as conn:
        conn.execute(ans_table.insert(), data)
        conn.commit()


if __name__ == "__main__":
    _config = {}
    try:
        _config = get_config_file()
        get_files_to_process(_config)
    except FileNotFoundError:
        print('Config File Not Found -> Exiting')
        sys.exit(1)

    volla = None
    try:
        volla = Volla()
        _host = os.environ.get('VOLLA_HOST')
        _username = os.environ.get('VOLLA_USER')
        _password = os.environ.get('VOLLA_PASS')
        volla.connect(_host, _username, _password)
    except TimeoutError:
        print('Connection to VollaPhone Error -> Exiting')
        sys.exit(1)

    TEST = False if os.environ.get('TEST', 'TRUE') != 'TRUE' else True

    info = []
    for key, value in _config.items():
        filename = os.path.join(os.getcwd(), os.environ.get('ORIGIN'), value.get('filename'))
        mensaje = value.get('mensaje')
        recipients = get_recipients(filename)
        if not recipients:
            print('Sin teléfonos a los que enviar')
            sys.exit(1)
        _info = send_to_recipients(volla, recipients, mensaje, TEST)
        info.extend(_info)

        if not TEST:
            os.remove(filename)

    if not TEST:
        os.remove(os.path.join(os.getcwd(), os.environ.get('ORIGIN'), 'config.yaml'))

    load_data_into_postgres(info)
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -> Finished OK")
