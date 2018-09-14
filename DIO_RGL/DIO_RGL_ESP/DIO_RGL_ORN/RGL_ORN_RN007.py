# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:ORN_RN007
#GLOSA: VALIDEZ SI (si es SI) o NO ( Si en N)
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 12/06/2018
#AUTOR: MTS

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("ORN_RN007",UDF_ORN_RN007("<nombre columna en la cual se debe aplicar la regla>"))
#UDF__ORN_RN007 = udf(ORN_RN007,StringType())

#DEFINICIÓN DE LA FUNCION QUE EVALUA SI EL TERMINO ES S (SI ES SI) O N (SI ES NO)
def RGL_ORN_RN007(Input):
    #EVALUA SI EL DATO ES S O N
    if Input == "SI":
        return 1
    else:
        return 0
