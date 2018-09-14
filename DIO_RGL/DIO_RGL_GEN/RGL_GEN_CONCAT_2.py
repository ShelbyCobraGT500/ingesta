# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: X
#GLOSA: CONCATENA LOS VALORES QUE VENGAN EN LAS COLUMNAS DE ENTRADA
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 12/06/2018
#AUTOR: MIGUEL CHOQUE

#INSTRUCCIONES DE COMO CREAR UDF PARA PYSPARK
#df_final = df_new.withColumn("RN001", UDF_RN001("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN001 = udf(RN001,StringType())
def RGL_GEN_CONCAT_2(col1,col2):
    try:
        if col1 != "":
            return col1+col2
        else:
            return 0
    except ValueError:  
        return 0