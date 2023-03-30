#!/usr/bin/python3

import sys


gastos_por_persona = {}
for linea in sys.stdin:
    persona, cantidad = linea.strip().split(";")
    gasto = int(cantidad)
    if persona not in gastos_por_persona:
        gastos_por_persona[persona] = 0
    gastos_por_persona[persona] += gasto
for persona, gasto in gastos_por_persona.items():
    print(persona + ";" + str(gasto))


