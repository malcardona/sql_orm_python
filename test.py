#!/usr/bin/env python

"""SQL Introducción [Python]
Ejercicio de profundizacion
---------------------------
Autor: malcardona
Version: 1.0

MELI API [Python]"""

import json
import requests
import sqlite3
import os.path
import csv

def create_schema():

    # Conectarnos a la base de datos
    # En caso de que no exista el archivo se genera
    # como una base de datos vacia
    with sqlite3.connect('MeLi.db') as db:
        # Crear el cursor para poder ejecutar las querys
        c = db.cursor()

    # Ejecutar una query
    c.execute("""
                DROP TABLE IF EXISTS meli;
            """)

    # Ejecutar una query
    c.execute("""
            CREATE TABLE meli(
                [id] TEXT PRIMARY KEY,
                [site_id] TEXT,
                [title] TEXT,
                [price] INTEGER,
                [currency_id] TEXT,
                [initial_quantity] INTEGER,
                [available_quantity] INTEGER,
                [sold_quantity] INTEGER
            );
            """)

    # Para salvar los cambios!
    db.commit()

def get_csv_dat(file):
    csv_path = os.path.dirname(os.path.abspath(__file__))
    t_file = os.path.join(csv_path, file)
    with open(t_file) as fi:
        data = list(csv.DictReader(fi))
        ids= []
        for row in data:
            if row["site"] != "MLA":
                continue
            else:
                ids.append(row["site"]+row["id"])
    return ids

def fillurl(list):
    urls =[]
    for i in list:
        urls.append(f"https://api.mercadolibre.com/items?ids={i}")
    return urls

def extract(url):
    # Extraer el JSON de la URL pasada
    # como parámetro
    response = requests.get(url)
    data = response.json()
    return data

def allval(data, key):
    if key in data:
        dat = data.copy()
    else:
        dat = {}
    return dat

def listfjson(data):
    dat1 = [x for x in data]
    k_list = [ "id", "site_id", "title", "price", "currency_id", "initial_quantity", "available_quantity", "sold_quantity"]
    dat1 = dat1[0]
    dat2 = dat1.get('body')
    da_set1 = dict(filter(lambda x: x[0] in k_list, dat2.items()))
    for key in k_list:
        da_set = allval(da_set1, key)   
        list1= da_set.values()
    return list1

def get_list(csv_f):
        l1= get_csv_dat(csv_f)
        l2 = fillurl(l1)
        db_l = []
        for url in l2:    
            data = extract(url)
            list1 = listfjson(data) 
            db_l.append(list1)
            listdb = [x for x in db_l if x]
            listdb.pop()
        print(listdb)
        return listdb

def fill(db,url):
   with sqlite3.connect(db) as db:
        # Crear el cursor para poder ejecutar las querys
        c = db.cursor() 
        data = extract(url)
        list1 = list(listfjson(data))
        c.execute("""
                        INSERT INTO meli (id, site_id, title, price, currency_id, initial_quantity, available_quantity, sold_quantity)
                        VALUES (?,?,?,?,?,?,?,?);""", list1)
        db.commit()
            
           
def fetch(id):
       with sqlite3.connect('MeLi.db') as db:
        # Crear el cursor para poder ejecutar las querys
            c = db.cursor()
            query = c.execute('SELECT * FROM meli WHERE id =?', (id,)).fetchall()
            if query != []:
                print(query)
            else:
                print("No existe el Id especificado en la base de datos")

if __name__ == "__main__":

    # Crear DB
    create_schema()

    # Completar la DB con el CSV
    fill('MeLi.db', 'https://api.mercadolibre.com/items?ids=MLA845041373')

    # Leer filas
    fetch('MLA845041373')
    fetch('MLA717159516')

    

