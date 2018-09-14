# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:TG_1_005 ()
#GLOSA: 99 Chileno Exterior (No región)
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE AVALOS DÍAZ 


def TG_1_005(Input):
    #Evalua si el dato es igual a 99
    try:
        if int(Input) == 99:
            return 1
        else:
            return 0
    except:
        return 0
