# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN004 (ID=30)
#GLOSA: Tipo de actividad del cliente 02, 06 y 07
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 20/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA
#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN004",UDF_RN004_BTG("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN030 = udf(RN004,StringType())

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_BTG_RN004(Input):
    try:
        if Input == '02' or Input == '06' or Input == '07':    
            return 1 # si el dato es distinto a '00'
        else:
            return 0 #en caso contrario
    except:
        return 0