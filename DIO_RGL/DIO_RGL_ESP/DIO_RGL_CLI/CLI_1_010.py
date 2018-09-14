# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_1_009
#GLOSA: RUT ENTRE 50.000.000 Y 199.999.999
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE AVALOS DÍAZ 

import re

def CLI_1_010(Input, Input1):
    #EVALUA SI EL DATO ES IGUAL A 1
    try:
        if Input >= "50000000" and Input <= "199999999": 
            if Input == "199999999":
                if  re.match("^[0-9]{1}",Input1):
                    return 1
                else:
                    return 0
            else:
                return 1
        else:
            return 0
    except:
        return 0