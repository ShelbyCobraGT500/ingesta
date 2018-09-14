# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:BTG_RN008 ()
#GLOSA: Pais distinto a 160
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 20/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN008",UDF_RN008("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN030 = udf(RN008,StringType())

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_BTG_RN008(Input):
    #Evalua si el dato es distinto a 160
    try:
        if int(Input) <> 160:
            return 1
        else:
            return 0
    except:
        return 0