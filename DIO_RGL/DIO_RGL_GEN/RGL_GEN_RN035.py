# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN033 (ID=35)
#GLOSA: DATO DEBE SER S O N
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN035",UDF_RN035("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN035 = udf(RN035,StringType())


#Definicion de la funcion que comprueba si el caracter es S o N
def RGL_GEN_RN035(Input):
    #validacion si el caracter es S o N
    if Input.upper() == "S" or Input.upper() == "N":
        return 1
    else:
        return 0