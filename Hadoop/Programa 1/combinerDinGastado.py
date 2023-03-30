#!/usr/bin/python3

import sys


gastos_por_persona = {}
for linea in sys.stdin:
    persona, cantidad = linea.strip().split(";")
    gasto = int(cantidad)
    if persona not in gastos_por_persona:
        gastos_por_persona[persona] = []
    gastos_por_persona[persona].append(gasto)
for persona, gastos in gastos_por_persona.items():
    print(persona + ";" + str(sum(gastos)))
