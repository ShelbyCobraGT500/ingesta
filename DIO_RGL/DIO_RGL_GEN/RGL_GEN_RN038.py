# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN038 (ID=38)
#GLOSA: VALIDAR SI DATO NUMERICO ES 1, 2, 3 O 9
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN038",UDF_RN038("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN038 = udf(RN038,StringType())

#deficion que evalua si el dato numeri es 1 o 2 o 3 o 9
def RGL_GEN_RN038(Input):
    try:
        dato= int(Input)
        if dato == 1 or dato == 2 or dato == 3 or dato == 9:
            return 1
        else:
            return 0
    except:
        return 0