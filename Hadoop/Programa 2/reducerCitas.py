#!/usr/bin/python3
import sys

current_cited = None
citing_patents = []

# Recorre cada línea de la entrada estándar
for line in sys.stdin:
    # Elimina espacios en blanco y divide la línea en la clave y el valor
    line = line.strip()
    cited, citing = line.split("\t")

    # Si la clave actual es distinta a la clave anterior, se escribe el resultado final
    if current_cited is not None and current_cited != cited:
        citing_patents.sort()
        citing_patents_str = ",".join(citing_patents)
        print(current_cited + "\t" + citing_patents_str)
        citing_patents = []

    # Se agrega la patente citante a la lista correspondiente
    current_cited = cited
    citing_patents.append(citing)

# Se escribe el resultado final para la última patente citada
if current_cited:
    citing_patents.sort()
    citing_patents_str = ",".join(citing_patents)
    print(current_cited + "\t" + citing_patents_str)
