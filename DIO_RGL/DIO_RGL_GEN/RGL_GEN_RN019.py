# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN019 (ID=19)
#GLOSA: NO DEBE TENER DECIMALES
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN0019",UDF_RN019("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN019 = udf(RN019,StringType())

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_GEN_RN019(Input):
#Evalua si el dato NO tiene decimales
    try:
        if Input.isdigit() or int(Input):
            return 1 # si el dato es digito y es entero
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0