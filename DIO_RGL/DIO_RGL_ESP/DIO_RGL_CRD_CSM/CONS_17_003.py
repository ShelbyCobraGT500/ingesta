# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: CONS_17_003
#GLOSA: NO DEBE CONTENER NULOS YA SEA NULO ASCII "\x00" O NULO SQL "NULL"
#COMPLEJIDAD: BAJA (porque aplica a tres EDC)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 04/09/2018
#Autor: Marcos Toro Saldia

#Definicion: funcion que evalua si cada uno de los datos que conforman el dato calculado no sea nulo.
import re

def CONS_17_003(Input1, Input2, Input3):
    #Evalua si el dato es nulo
    try:
        if re.search("\x00",Input1) and re.search("\x00",Input2) and re.search("\x00",Input3):
            return 0 # si esta nulo ('NULL')"
        else:
            return 1 # en caso contrario
    except:
        return 0