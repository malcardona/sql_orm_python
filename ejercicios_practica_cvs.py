#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import sqlite3
import csv
import os.path

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///secundaria.db")
base = declarative_base()


class Tutor(base):
    __tablename__ = "tutor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Tutor: {self.name}"


class Estudiante(base):
    __tablename__ = "estudiante"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    grade = Column(Integer)
    tutor_id = Column(Integer, ForeignKey("tutor.id"))

    tutor = relationship("Tutor")

    def __repr__(self):
        return f"Estudiante: {self.name}, edad {self.age}, grado {self.grade}, tutor {self.tutor.name}"


def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)


def insert_tutor(name):
    # Crear la session
    Session = sessionmaker(bind=engine)
    t_data = Session()

    # Crear un nuevo tutor
    tutor = Tutor(name=name)

    # Agregar El nuevo tutor a la DB
    t_data.add(tutor)
    t_data.commit()
    print(tutor)

def insert_est(name, age, grade, tutor_id):
    # Crear la session
    Session = sessionmaker(bind=engine)
    in_data = Session()

    # Buscar el tutor
    query = in_data.query(Tutor).filter(Tutor.id == tutor_id)
    id_T = query.first()

    # Crear la persona
    estudiante = Estudiante(name=name, age=age, grade=grade)
    estudiante.tutor = id_T

    # Agregar la persona a la DB
    in_data.add(estudiante)
    in_data.commit()
    print(estudiante)

def fill():
    # Llenar la tabla de la secundaria con al menos 2 tutores
    # Cada tutor tiene los campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del tutor (puede ser solo nombre sin apellido)

    # Obtener la path de ejecución actual del csv
    csv_path = os.path.dirname(os.path.abspath(__file__))
    t_file = os.path.join(csv_path, "tutores.csv")
    with open(t_file) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            insert_tutor(row['tutor'])

    # Llenar la tabla de la secundaria con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # tutor --> el tutor de ese estudiante (el objeto creado antes)

    # Obtener la path de ejecución actual del csv
    csv_path = os.path.dirname(os.path.abspath(__file__))
    e_file = os.path.join(csv_path, 'estudiantes.csv')
    with open(e_file) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            insert_est(row['name'], int(row['age']), int(row['grade']), int(row['tutor_id']),)

    # No olvidarse que antes de poder crear un estudiante debe haberse
    # primero creado el tutor.


def fetch():
    print('Comprovemos su contenido, ¿qué hay en la tabla?')
    # Crear una query para imprimir en pantalla
    # todos los objetos creados de la tabla estudiante.
    # Imprimir en pantalla cada objeto que traiga la query
    # Realizar un bucle para imprimir de una fila a la vez
    Session = sessionmaker(bind=engine)
    q_data = Session()

    dat_tab = q_data.query(Estudiante)

    for estudiante in dat_tab:
        print(estudiante)
    


def search_by_tutor(tutor):
    print('Operación búsqueda!')
    # Esta función recibe como parámetro el nombre de un posible tutor.
    # Crear una query para imprimir en pantalla
    # aquellos estudiantes que tengan asignado dicho tutor.

    # Para poder realizar esta query debe usar join, ya que
    # deberá crear la query para la tabla estudiante pero
    # buscar por la propiedad de tutor.name
    Session = sessionmaker(bind=engine)
    q_data = Session()

    result = q_data.query(Estudiante).join(Estudiante.tutor).filter(Tutor.name == tutor)

    for estudiante in result:
        print(estudiante)

def modify(id, name):
    print('Modificando la tabla')
    # Deberá actualizar el tutor de un estudiante, cambiarlo para eso debe
    Session = sessionmaker(bind=engine)
    q_data = Session()

    # 1) buscar con una query el tutor por "tutor.name" usando name
    # pasado como parámetro y obtener el objeto del tutor

    obj_t = q_data.query(Tutor).filter(Tutor.name==name)
    tname = obj_t.first()

    # 2) buscar con una query el estudiante por "estudiante.id" usando
    # el id pasado como parámetro

    obj_e = q_data.query(Estudiante).filter(Estudiante.id== id)
    ename = obj_e.first()

    # 3) actualizar el objeto de tutor del estudiante con el obtenido
    # en el punto 1 y actualizar la base de datos

    print(f'A el {ename} se le asignara como nuevo {tname}')

    ename.tutor = tname

    q_data.add(ename)

    q_data.commit()

    # TIP: En clase se hizo lo mismo para las nacionalidades con
    # en la función update_persona_nationality


def count_grade(grade):
    print('Estudiante por grado')
    # Utilizar la sentencia COUNT para contar cuantos estudiante
    # se encuentran cursando el grado "grade" pasado como parámetro
    # Imprimir en pantalla el resultado
    Session = sessionmaker(bind=engine)
    q_data = Session()

    c_grade = q_data.query(Estudiante).filter(Estudiante.grade== grade).count()

    print(f' En el grado {grade} hay {c_grade} estudiantes')

    # TIP: En clase se hizo lo mismo para las nacionalidades con
    # en la función count_persona


if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    create_schema()   # create and reset database (DB)
    fill()
    fetch()

    tutor = 'Lando-N'
    search_by_tutor(tutor)

    nuevo_tutor = 'Valteri-B'
    id = 2
    modify(id, nuevo_tutor)

    tutor = 'Valteri-B'
    search_by_tutor(tutor)

    grade = 2
    count_grade(grade)
