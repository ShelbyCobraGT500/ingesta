# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN005 (ID=05)
#Glosa: DATO NO DEBE CONTENER NI PUNTOS NI GUIONES
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN PYSPARK 2.X 
#FECHA MODIFICACIÓN: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN005",udf_RN005("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN005 = udf(RN005,StringType())

#Definicion de la funcion que comprueba que la cadena no tenga ni puntos ni caracteres
def RGL_GEN_RN005(Input):
    bandera = 0
    #ciclo que recorre el caracter hasta encontrar un . o un -
    for caract in Input:
        if caract == '.' or caract == '-':
            bandera = 1
            break
    #condicional que evalua si el Input posee . o - dependiendo de el valor de bandera en el caso de bandera es 1 quiere decir que si tiene puntos o -
    if bandera == 1:
        return 0
    else:
        return 1