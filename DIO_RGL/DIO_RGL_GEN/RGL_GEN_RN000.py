# coding=utf-8
#CÓDIGO DE LA REGLA DE CALIDAD: RN000
#GLOSA: EVALUA SI EL LARGO DEL DATO CONTENIDO EN EL DATAFRAME COINCIDE CON EL LARGO ESTIPULADO EN LA METADATA
#PROBADO EN Pyspark 2.x
#FECHA MODIFICACIÓN: 22/06/2018
#AUTOR: FELIPE ÁVALOS

#INSTRUCCIONES DE COMO CREAR UDF PARA PYSPARK
#df_final = df_new.withColumn("RN000", UDF_RN000("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN000 = udf(RN000,StringType())


#definición de la función que evalua si la columna contiene NULOS
def RGL_GEN_RN000(Input,Size):
  #condicional que evaluá si cumple con los parametros establecidos, transforma el dato de Unicode a Ascii
  if len(Input) == int(Size):
      return 1 # si el largo coincide
  else:
      return 0 # en caso contrario