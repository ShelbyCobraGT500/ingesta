# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN029 (ID=29)
#GLOSA: DATO DEBE SER DISTINTO DE 0
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#UDF_RN029 = udf(RN029,StringType())
#df_final = df_new.withColumn("RN029",UDF_RN029("<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_GEN_RN029(Input):
    #Evalua si el dato es diferente a '0'
    try:
        if Input != '0' and len(Input.strip(" ")) == 1:
            return 1 # si el dato es distinto a '0'
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0