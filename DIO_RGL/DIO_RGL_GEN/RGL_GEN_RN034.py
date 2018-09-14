# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN034 (ID=34)
#GLOSA: VALIDAR SI DATO NUMERICO ES 1, 2, 3, 4 O 9
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN034",UDF_RN034("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN034 = udf(RN034,StringType())

#deficion que evalua si el dato numeri es 1,2,3,4 o 9
def RGL_GEN_RN034(Input):
    if Input == "1" or Input == "2" or Input == "3" or Input == "4" or Input == "9":
        return 1
    else:
        return 0