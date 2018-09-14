# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN018 (ID=18)
#GLOSA: DATO DEBE SER NUMERICO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN018",UDF_RN018("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN018 = udf(RN018,StringType())

#Definicion: funcion que evalua si el dato es numerico
def RGL_GEN_RN018(Input):
    #Evalua si el dato es numerico
    try:
        if Input.isdigit() or float(Input):
            return 1 # si el dato es numerico
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0