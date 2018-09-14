# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN045
#GLOSA: Es valido si el dato es numerico
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 07/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN045",UDF_RN045("<nombre de las columnas en la cual se debe aplicar la regla deben ir en formato año,mes,dia>")) 
#UDF_RN045 = udf(RN045,StringType())

def RGL_GEN_RN045(Input):
    try:
        if float(Input):
            return 1
        else:
            return 0    
    except ValueError:
        return 0
