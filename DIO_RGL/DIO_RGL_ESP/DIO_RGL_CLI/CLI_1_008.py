# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_1_008
#GLOSA: RUT ENTRE 20010000 Y 20050000
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE AVALOS DÍAZ 


def CLI_1_008(Input, Input1):
    #EVALUA SI EL DATO ES IGUAL A 1
    try:
        if Input >= "20010000" and Input <= "20050000": 
            if Input == "20050000":
                if Input1 == "0":
                    return 1
                else:
                    return 0
            else:
                return 1
        else:
            return 0
    except:
        return 0