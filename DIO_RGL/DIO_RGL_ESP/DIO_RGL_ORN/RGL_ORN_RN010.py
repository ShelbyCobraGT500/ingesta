# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN003 (ID=3)
#GLOSA: NO CONTENGA ESPACIOS
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACION: 30/05/2018
#AUTOR: FELIPE AVALOS
#EJEMPLO DE CREACION DE UDF
#df_final = df_new.withColumn("RN003",UDF_RN003(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN003 = udf(RN003,StringType())

#Si UNI-CMN-LOC-COD-R en T103UNI existe en COM-COD-R en T103COM.

#Definicion: funcion que evalua si el dato NO contiene espacios.
def RGL_ORN_RN010(Input):
  #condicional que evaluá si cumple con los parametros establecidos, transforma el dato de Unicode a Ascii
  if re.search(" ",Input):
    return 0 # si no esta vació "", blanco" "
  else:
    return 1 # en caso contrario
