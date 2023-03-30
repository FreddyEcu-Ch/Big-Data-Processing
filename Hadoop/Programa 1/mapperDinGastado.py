#!/usr/bin/python3

import sys

for linea in sys.stdin:
    persona, producto, cantidad, esDevolucion = linea.strip().split(";")
    if esDevolucion == "Cierto":
        cantidad = -int(cantidad)
    else:
        cantidad = int(cantidad)
    print(persona + ";" + str(cantidad))