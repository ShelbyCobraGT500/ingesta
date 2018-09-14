# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_17_006
#GLOSA: EVALUA QUE UNA VARIABLE DE LARGO X
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 04/09/2018
#AUTOR: Marcos Toro Saldia

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("CONS_17_006"},UDF_CONS_17_006(<nombre de las columnas en la cual se debe aplicar la regla>,x(donde x es el tamaño a evaluar)))
#UDF_CONS_17_006 = udf(CONS_17_006,StringType())

#definicion de la funcion para evaluar tamaño del Input
def CONS_17_006(Input1,Input2,Input3,size):
    try:
        tam = int(size)
        if (len(str(Input1)) == tam) and (len(str(Input2)) == tam) and (len(str(Input3)) == tam):
            return 1
        else:
            return 0
    except ValueError:  
        return 0

    