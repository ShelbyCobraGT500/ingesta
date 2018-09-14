# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_56_001 
#GLOSA: VALIDAR RIESGO O DECLARADA
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 10/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ


#DEFINICIÓN DE LA FUNCION QUE EVALUA SI LA FECHA ES DISTINTA DE 1900/01/01
def CLI_56_001(Input):
    if Input.upper() == "RIESGO" or Input.upper() == "DECLARADA":
        return 1
    else:
        return 0