#!/usr/bin/python3

import sys

from pyspark import SparkConf, SparkContext


# Función para mapear las personas que gastaron una cantidad mayor a 1500 euros
def contar(lista):
    if int(lista[2]) > 1500:
        return lista[0], 1
    else:
        return lista[0], 0

# Crear configuración de Spark y contexto
conf = SparkConf().setAppName("personaYMetodosDePago")
sc = SparkContext(conf=conf)

# Ruta de datos de entrada y salida
entrada = sys.argv[1]
salida = sys.argv[2]

# Cargar los datos de entrada en un RDD
data = sc.textFile(entrada)

# Primera transformación convirtiendo las entradas en tuples
rdd = data.map(lambda x: tuple(x.split(";")))

# Segunda transformación para Filtrar las compras sido pagadas con un método de pago diferente a tarjeta de crédito
rdd = rdd.filter(lambda x: x[1] != "Tarjeta de crédito")

# Tercera transformación para convertir las entradas en tuples con el formato (persona, 1) o en formato (persona, 0)
# dependiendo del valor gastado por cada persona
rdd = rdd.map(contar)

# Cuarta transformación para calcular el número de compras realizadas por cada persona
rdd = rdd.reduceByKey(lambda x, y: x + y)

# Quinta transformación para convertir las entradas en cadenas de caracteres con el formato "persona; contador"
rdd = rdd.map(lambda x: "{};{}".format(x[0], x[1]))

# Guardado del RDD resultante en un archivo de texto
rdd.saveAsTextFile(salida)

# Terminar el programa
sc.stop()
