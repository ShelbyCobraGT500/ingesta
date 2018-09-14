# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN037 (ID=37)
#GLOSA: DEBE SER MAYOR A 0
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN037",UDF_RN037("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN037 = udf(RN037,StringType())

#Definicion: funcion que evalua si el dato es mayor a cero
def RGL_GEN_RN037(Input):
    #Evalua si el dato es mayor a cero
    try:
        if float(Input) > 0:
            return 1 # si el dato es mayor a cero
        else:
            return 0 #en caso contrario
    except:
        return 0