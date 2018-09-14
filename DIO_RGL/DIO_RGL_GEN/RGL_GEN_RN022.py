# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN022 (ID=22)
#GLOSA: DATO DEBE SER MAYOR O IGUAL A 1
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN022",UDF_RN022("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN022 = udf(RN022,StringType())

#Definicion: funcion que evalua si el dato es mayor a igual a uno
def RGL_GEN_RN022(Input):
    #Evalua si el dato es mayor igual a uno
    try:
        if float(Input) >= 1:
            return 1 # si el dato es mayor o igual a uno
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0