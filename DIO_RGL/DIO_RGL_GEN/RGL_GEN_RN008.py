# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN008 (ID=8)
#GLOSA: FECHA VALIDA (YYYY-MM-DD)
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN008",UDF_RN008("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN008 = udf(RN008,StringType())

from datetime import datetime

#definicion de la funcion que evalua si Fecha válida (año-mes-día), recibe tres parametros en el siguinete orden : año,mes,dia
def RGL_GEN_RN008(century,year,month,day):
    #try en caso de que no venga en el orden año-mes-dia, si no viene en ese orden retornara 0
    try:
        #se crea un estring que contenga la fecha en formato s/y/m/d
        formato_fecha = '%Y/%m/%d'
        date = century + year + "/" + month + "/"+ day
        #conversion de la fecha a formato fecha
        #condicional que evalua si la fecha es valida o no
        if datetime.strptime(date,formato_fecha):
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except ValueError:
        return 0