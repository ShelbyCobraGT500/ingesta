# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_3_001
#GLOSA: VLOR ENTRE 0 O 9
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

import re

def CLI_3_001(Input):
    try:
        if  re.match("^[0-9]{1}",Input):
            return 1
        else:
            return 0
    except:
        return 0