# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN020 (ID=20)
#GLOSA: DEBE SER UN PORCENTAJE VALIDO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 05/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN020",UDF_RN020(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN020 = udf(RN020,StringType())

#Definicion de la funcion que comprueba si es un porcentaje valido
def RGL_GEN_RN020(Input):
    #try en caso de que se presente un error
    try:
        #validacion si el porcentaje usado es valido
        if float(Input) >= 0 and float(Input)<=100:
            return 1
        else:
            return 0
    except ValueError:
        return 0