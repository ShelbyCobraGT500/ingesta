# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: CONS_17_004
#GLOSA: Los datos sumados no pueden ser blancos
#COMPLEJIDAD: BAJA (porque aplica a tres EDC)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 18/07/2018
#Autor: Marcos Toro Saldia

#Definicion: funcion que evalua si cada uno de los datos que conforman el dato calculado no sea nulo.

def CONS_17_004(Input1,Input2,Input3):
  try:
    Input1=Input1.strip()
    Input2=Input2.strip()
    Input3=Input3.strip()
    if Input1 and Input2 and Input3:
      return 1 # campo correcto
    else:
      return 0 # en caso contrario
  except:
    return 0
    