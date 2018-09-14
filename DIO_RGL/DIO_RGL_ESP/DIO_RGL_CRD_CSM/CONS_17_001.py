# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_17_001
#GLOSA: Sumatoria de campos debe ser mayor a cero
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/09/2018
#AUTOR: Marcos Toro Saldia


#Definicion: funcion que evalua si el dato es mayor a cero
def CONS_17_001(Input1, Input2, Input3):
    #Evalua si el dato es mayor a cero
    Suma = int(Input1) + int(Input2) + int(Input3)
    try:
        if Suma > 0:
            return 1 # si el dato es mayor a cero
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0