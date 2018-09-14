# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_11_003 
#GLOSA: FECHA DISTINTA A 1900/01/01
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ


from datetime import datetime

#DEFINICIÓN DE LA FUNCION QUE EVALUA SI LA FECHA ES DISTINTA DE 1900/01/01
def CLI_11_003(date):
    try:
        #condicional que evalua si la fecha es mayor a 1900/01/01
        if date > "19000101":
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except:
        return 0