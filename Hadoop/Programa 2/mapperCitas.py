#!/usr/bin/python3
import sys

# Recorre cada línea de la entrada estándar
for line in sys.stdin:
    if not line.startswith("CITING"):

        # Elimina espacios en blanco y divide la línea en los campos "citing" y "cited"
        line = line.strip()
        citing, cited = line.split(",")

        # Genera la pareja clave-valor intermedia invertida y escribe en la salida estándar
        print(cited + "\t" + citing)
