# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN009
#GLOSA: EVALUA QUE UNA VARIABLE DE LARGO X
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 16/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN009"},UDF_RN009(<nombre de las columnas en la cual se debe aplicar la regla>,x(donde x es el tamaño a evaluar)))
#UDF_RN009 = udf(RN009,StringType())

#definicion de la funcion para evaluar tamaño del Input
def RGL_GEN_RN009(Input,size):
    try:
        tam = int(size)
        if len(str(Input)) == tam:
            return 1
        else:
            return 0
    except ValueError:  
        return 0