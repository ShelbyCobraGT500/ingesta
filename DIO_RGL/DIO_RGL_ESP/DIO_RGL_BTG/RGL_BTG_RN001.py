# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RGL_BTG_RN001 ()
#GLOSA: 999 Chileno Exterior (No región)
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 19/07/2018 NLLANOS
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN001",UDF_RN001("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN030 = udf(RN001,StringType())

def RGL_BTG_RN001(Input):
    #Evalua si el dato es igual a 999
    try:
        if int(Input) == 999:
            return 1
        else:
            return 0
    except:
        return 0