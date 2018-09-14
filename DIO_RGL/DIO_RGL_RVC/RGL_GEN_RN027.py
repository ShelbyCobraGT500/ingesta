# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN027 (ID=27)
#GLOSA: DATO DEBE SER OBLIGATORIO DE LARGO 8
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 01/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ
#ÚLTIMA MODIFICACIóN: MTS 

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#UDF_RN027 = udf(RN027,StringType())
#df_final = df_new.withColumn("RN027",UDF_RN027("<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el Input es de largo 8
def RGL_GEN_RN027(Input):
    #Validación si el dato es de largo 8 (se elimina función strip(" ")) 
    if len(Input)==8:
        return 1
    else:
        return 0