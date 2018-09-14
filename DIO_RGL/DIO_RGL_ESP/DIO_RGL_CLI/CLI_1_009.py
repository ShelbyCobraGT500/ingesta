# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_1_009
#GLOSA: CLI_TIP_COD =3 o CLI_TIP_COD =4 
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE AVALOS DÍAZ 

def CLI_1_009(Input):
    #EVALUA SI EL DATO ES IGUAL A 3 O 4
    try:
        if Input == "3" or Input == "4":
            return 1
        else:
            return 0
    except:
        return 0