# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_1_007
#GLOSA: CLI_TIP_COD =1
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE AVALOS DÍAZ 


def CLI_1_007(Input):
    #EVALUA SI EL DATO ES IGUAL A 1
    try:
        if Input == "1":
            return 1
        else:
            return 0
    except:
        return 0