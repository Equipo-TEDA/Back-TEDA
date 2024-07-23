import json
from sqlalchemy import create_engine, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Cargar configuración desde config.json
with open('config/config.json') as config_file:
    config = json.load(config_file)

# Variables de configuración
DB_NAME = config['DB_NAME']
DB_HOST = config['DB_HOST']
DB_PASSWORD = config['DB_PASSWORD']
DB_DIALECT = config['DB_DIALECT']
DB_USER = config['DB_USER']

# Construir la URL de conexión a la base de datos
URL_CONNECTION = "{}://{}:{}@{}/{}".format(DB_DIALECT, DB_USER, DB_PASSWORD, DB_HOST, DB_NAME)

# Crear el motor de SQLAlchemy y la sesión
engine = create_engine(URL_CONNECTION)
local_session = sessionmaker(autoflush=False, autocommit=False, bind=engine)