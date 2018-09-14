# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN010
#GLOSA: EVALUA QUE UNA VARIABLE SEA NUMERICO ENTERO
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 16/06/2018
#AUTOR: MIGUEL CHOQUE HERRRERA

#definicion de la funcion para evaluar tamaño del Input
def RGL_GEN_RN010(Input):
    try:
        if int(Input):
            return 1
        else:
            return 0
    except:  
        return 0