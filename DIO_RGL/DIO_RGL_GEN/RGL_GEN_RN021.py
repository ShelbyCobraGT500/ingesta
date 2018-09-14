# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN021 (ID=21)
#GLOSA: VALIDAR QUE EL DATO SEA ENTERO 
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN021",UDF_RN021("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN021 = udf(RN021,StringType())


#deficion que evalua si el dato numerico es 1 o 2 o 3 o 9
def RGL_GEN_RN021(Input):
    try:
        if int(Input):
            return 1
        else:
            return 0
    except:
        return 0