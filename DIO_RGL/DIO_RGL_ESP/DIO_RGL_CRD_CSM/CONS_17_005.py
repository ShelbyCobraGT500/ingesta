# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_17_005
#GLOSA: EVALUA QUE cada uno de los campos que forman el dato sean tipo numerico.
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 04/09/2018
#AUTOR: Marcos Toro Saldia

#definicion de la funcion para evaluar tamaño del Input
def CONS_17_005(Input1,Input2,Input3):
    try:
        Input4=Input1+Input2+Input3
        int(Input4)
        return 1
    except:  
        return 0

    