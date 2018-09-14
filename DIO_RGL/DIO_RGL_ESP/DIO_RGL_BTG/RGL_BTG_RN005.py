# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:BTG_RN005 (ID=BTG_05)
#GLOSA: DEBE SER MAYOR A 00 Y MENOR QUE 100 (0 < DATO < 100)
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("BTG_RN005",UDF_BTG_RN005("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_BTG_RN005 = udf(RN005,StringType())

#Definicion: funcion que evalua si el dato es mayor a cero y menor a cien
def RGL_BTG_RN005(Input):
    #Evalua si el dato es mayor a cero y menor a cien
    try:
        if float(Input) > 0 and float(Input) < 100:
            return 1 # si el dato es mayor a cero y menor a cien
        else:
            return 0 #en caso contrario
    except:
        return 0