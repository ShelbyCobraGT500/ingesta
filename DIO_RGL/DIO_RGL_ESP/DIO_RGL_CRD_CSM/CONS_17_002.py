# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_17_002
#GLOSA: DEBE SER MAYOR A 0
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ


#Definicion: funcion que evalua si el dato es mayor a cero
def CONS_17_002(Input1, Input2, Input3):
    #Evalua si el dato es mayor a cero
    try:
        if Input1 > "0" and Input2 > "0" and Input3 > "0":
            return 1 # si el dato es mayor a cero
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0