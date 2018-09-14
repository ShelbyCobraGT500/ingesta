# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_1_006
#GLOSA: RUT PERSONA NATURAL < A 50MM
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE AVALOS DÍAZ 


def CLI_1_006(Input):
    #EVALUA SI EL DATO ES MENOR A 50MM
    try:
        if Input < "50000000":
            return 1
        else:
            return 0
    except:
        return 0