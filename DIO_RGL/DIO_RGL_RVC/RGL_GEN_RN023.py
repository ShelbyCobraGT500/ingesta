# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN023 (ID=23)
#GLOSA: LARGO MINIMO UN CARACTER
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN023",UDF_RN023("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN023 = udf(RN023,StringType())
import re
#Definicion de la funcion que comprueba si el gargo del dato es de minimo un caracter alfanumerico
def RGL_GEN_RN023(Input):
    #validacion largo minimo es de un carácter alfanumerico
    try:
        if re.match("^[a-zA-Z0-9]{1}",Input):
            return 1
        else:
            return 0
    except:
        return 0