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


def fill():
    print('Completemos esta tablita!')
    # Llenar la tabla de la secundaria con al menos 2 tutores
    # Cada tutor tiene los campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del tutor (puede ser solo nombre sin apellido)
    Session = sessionmaker(bind=engine)
    in_data = Session()

    tutor1 = Tutor(name= 'Max')
    in_data.add(tutor1)
    tutor2 = Tutor(name= 'Lewis')
    in_data.add(tutor2)
    tutor3 = Tutor(name= 'Valteri')
    in_data.add(tutor3)
    tutor4 = Tutor(name= 'Lando')
    in_data.add(tutor4)
    tutor5 = Tutor(name= 'Checo')
    in_data.add(tutor5)
    tutor6 = Tutor(name= 'Carlos')
    in_data.add(tutor6)

    in_data.commit()

    # Llenar la tabla de la secundaria con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # tutor --> el tutor de ese estudiante (el objeto creado antes)

    Session = sessionmaker(bind=engine)
    in_data = Session()

    est1 = Estudiante(name='SALMAN, Facundo', age=5, grade=1, tutor_id =5,)
    in_data.add(est1)
    est2 = Estudiante(name='ASTUDILLO, Luciano',age=7, grade=1, tutor_id =5)
    in_data.add(est2)
    est3 = Estudiante(name='CASTRO, Emmanuel', age=6, grade=1, tutor_id =5)
    in_data.add(est3)
    est4 = Estudiante(name='AYBAR, Ana', age=8, grade=2, tutor_id =1,)
    in_data.add(est4)
    est5 = Estudiante(name='ALBORNOZ, Julieta', age=8, grade=2, tutor_id =1)
    in_data.add(est5)
    est6 = Estudiante(name='BRUNO, Laura', age=9, grade=2, tutor_id =1)
    in_data.add(est6)
    est7 = Estudiante(name='CABRAL, Jazmin', age=10, grade=3, tutor_id =2,)
    in_data.add(est7)
    est8 = Estudiante(name='FERLAUTO, Petra', age=11, grade=3, tutor_id =2)
    in_data.add(est8)
    est9 = Estudiante(name='FLORES, Marcela', age=12, grade=3, tutor_id =2)
    in_data.add(est9)
    est10 = Estudiante(name='FRESCO, Adriana', age=13, grade=4, tutor_id =6)
    in_data.add(est10)
    est11 = Estudiante(name='MERCADO, Yamila', age=13, grade=4, tutor_id =6)
    in_data.add(est11)
    est12 = Estudiante(name='GONZALEZ, Miguel', age=12, grade=4, tutor_id =6)
    in_data.add(est12)
    est13 = Estudiante(name='GONZALEZ, Nadia', age=14, grade=5, tutor_id =4,)
    in_data.add(est13)
    est14 = Estudiante(name='CARDENAS, Gaston', age=14, grade=5, tutor_id =4)
    in_data.add(est14)
    est15 = Estudiante(name='HERRERA, Jose', age=15, grade=5, tutor_id =4)
    in_data.add(est15)
    est16 = Estudiante(name='PUITA, Jonatha', age=14, grade=6, tutor_id =3,)
    in_data.add(est16)
    est17 = Estudiante(name='LORENZI, Fausto', age=14, grade=6, tutor_id =3)
    in_data.add(est17)
    est18 = Estudiante(name='ALTAMIRANO, Sofia', age=15, grade=6, tutor_id =3)
    in_data.add(est18)

    in_data.commit()

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

    tutor = 'Lando'
    search_by_tutor(tutor)

    nuevo_tutor = 'Valteri'
    id = 2
    modify(id, nuevo_tutor)

    tutor = 'Valteri'
    search_by_tutor(tutor)

    grade = 2
    count_grade(grade)
