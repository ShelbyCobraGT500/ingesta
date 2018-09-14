# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN054
#GLOSA: Es valido si el dato no contiene ceros
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 07/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN054",UDF_RN054("<nombre de las columnas en la cual se debe aplicar la regla deben ir en formato año,mes,dia>")) 
#UDF_RN054 = udf(RN054,StringType())

def RGL_GEN_RN054(Input):
    try:
        if ("0" in Input):
            return 1
        else:
            return 0    
    except ValueError:
        return 0    
