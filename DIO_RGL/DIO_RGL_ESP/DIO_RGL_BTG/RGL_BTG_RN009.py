# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN004 (ID=30)
#GLOSA:  debe ser distinto a 00000
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 04/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA
#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN009",UDF_RN009("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN009 = udf(RN009,StringType())

#Definicion: funcion que evalua si el dato no contiene decimales
def RGL_BTG_RN009(Input):
    #Evalua si el dato es diferente a '00'
    try:
        if Input != '00000':
            return 1    
        else:
            return 0 #en caso contrario
    except:
        return 0