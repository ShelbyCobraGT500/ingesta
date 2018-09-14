# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:BTG_RN001 ()
#GLOSA: Validez S (si es SI) o N ( Si en N)
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 12/06/2018
#AUTOR: FELIPE AVALOS DIAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("ORN_RN013",UDF_ORN_RN013("<nombre columna en la cual se debe aplicar la regla>"))
#UDF__ORN_RN030 = udf(ORN_RN013,StringType())

#DEFINICIÓN DE LA FUNCION QUE EVALUA SI EL TERMINO ES S (SI ES SI) O N (SI ES NO)
def RGL_ORN_RN013(Input):
    #EVALUA SI EL DATO ES S O N
    if Input == "S":
        return 1
    else:
        return 0

