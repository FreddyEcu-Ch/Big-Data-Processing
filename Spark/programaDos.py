#!/usr/bin/python3

import sys

from pyspark import SparkConf, SparkContext


# Función para extraer las listas cuya longitud sea de igual a 29
def listaCat(lista):
    if len(lista) == 29:
        return lista[3], int(lista[5])


# Función para extraer las columnas de interés
def selCat(lista):
    return lista[3], int(lista[5])


# Función para extraer la cantidad minia de vistas por categoria
def minVis(acum, valor):
    minim = acum
    if valor < minim:
        minim = valor
    return minim


# Crear configuración de Spark y contexto
conf = SparkConf().setAppName("CategoriaDeVideosMenosVista")
sc = SparkContext(conf=conf)

# Ruta de datos de entrada y salida
entrada = sys.argv[1]
salida = sys.argv[2]

# Cargar los datos de entrada en un RDD
datos = sc.wholeTextFiles(entrada)

# Primera transformación con flatMap
rdd = datos.flatMap(lambda x: x[1].split("\n"))

# Segunda transformación con map
rdd = rdd.map(lambda x: x.split("\t"))

# Tercera transformación filtrando las listas con mayor información
rdd = rdd.filter(listaCat)

# Cuarta transformación escogiendo con map los campos requeridos
rdd = rdd.map(selCat)

# Quinta transformación con reduceByKey para sumar las vistas por categoría
rdd = rdd.reduceByKey(lambda x, y: x + y)

# Sexta transformación para ordenar el RDD por valor en orden ascendente
rdd = rdd.sortBy(lambda x: x[1])

# obtener la primera tuple del RDD, que tendrá el valor mínimo
min_tuple = rdd.first()

# transformar la tuple resultante a la forma (clave, valor)
min_kv = (min_tuple[0], min_tuple[1])

# Séptima transformación para guardar el resultado en un archivo de texto
rdd = sc.parallelize([min_kv])

# Octava transformación para convertir las entradas en cadenas de caracteres con el formato "categoría; vistas"
rdd = rdd.map(lambda x: "{};{}".format(x[0], x[1]))

# Acción final
rdd.saveAsTextFile(salida)

# terminar el programa
sc.stop()