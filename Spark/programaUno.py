#!/usr/bin/python3

import sys

from pyspark import SparkConf, SparkContext

# Crear configuración de Spark y contexto
conf = SparkConf().setAppName("personSpentWithoutCreditCard")
sc = SparkContext(conf=conf)

# Ruta de datos de entrada y salida
entrada = sys.argv[1]
salida = sys.argv[2]

# Cargar los datos de entrada en un RDD
data = sc.textFile(entrada)

# Primera y segunda transformación, dividir la data en elementos individuales y filtrar los pagos con tarjeta de crédito
rdd = data.map(lambda x: x.split(";")).filter(lambda x: x[1] == "Tarjeta de crédito")

# Tercera transformación con map
rdd = rdd.map(lambda x: (x[0], float(x[2])))

# Cuarta transformación con reduceByKey, para sumar la cantidad gastada por persona
rdd = rdd.reduceByKey(lambda x=0, y=0: x + y)

# Quinta transformación para convertir las entradas en cadenas de caracteres con el formato "persona; contador"
rdd = rdd.map(lambda x: "{};{}".format(x[0], x[1]))

# Acción final
rdd.saveAsTextFile(salida)

# Terminar el programa
sc.stop()
