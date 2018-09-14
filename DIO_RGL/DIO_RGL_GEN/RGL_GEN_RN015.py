# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN015
#GLOSA: EVALUA QUE LA VARIABLE SEA DE LARGO 1 HASTA 10
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 16/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN015"},UDF_RN015(<nombre de las columnas en la cual se debe aplicar la regla>,x(donde x es el tamaño a evaluar)))
#UDF_RN009 = udf(RN015,StringType())

#definicion de la funcion para evaluar tamaño del Input
def RGL_GEN_RN015(Input):
    try:
        if len(str(Input)) >= 1 and len(str(Input)) <= 10:
            return 1
        else:
            return 0
    except ValueError:  
        return 0