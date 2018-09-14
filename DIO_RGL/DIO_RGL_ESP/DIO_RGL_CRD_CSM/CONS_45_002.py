# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_45_002
#GLOSA: SI la moneda del crédito es pesos (OPC-COD-MON=000), el valor del reajuste de la cuota debe ser igual a cero. 
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 08/08/2018
#AUTOR: MTS


#Definicion: funcion que evalua si el dato es mayor a cero
def CONS_45_002(Input1, Input2):
    #Evalua si el dato es mayor a cero
    try:
        if Input1 == "000" and int(Input2) == 0:
            return 1 # si el dato es mayor a cero
        else:
            return 0 #en caso contrario
    except:
        return 0