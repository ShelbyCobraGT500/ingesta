# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN007 (ID=7)
#GLOSA: FECHA DISTINTA A 1900/01/01
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN007",UDF_RN007("<nombre de las columnas en la cual se debe aplicar la regla deben ir en formato año,mes,dia>")) 
#UDF_RN007 = udf(RN007,StringType())

from datetime import datetime

#DEFINICIÓN DE LA FUNCION QUE EVALUA SI LA FECHA ES DISTINTA DE 1900/01/01
def RGL_GEN_RN007(century,year,month,day):
    try:
        formato_fecha = '%Y/%m/%d'
        fecha = datetime.strptime('1900/01/01',formato_fecha)
        #se crea un estring que contenga la fecha en formato s/y/m/d
        #newcentury = int(century) - 1
        date = century + year + "/" + month + "/"+ day
        #try en caso de que no venga en el orden año-mes-dia, si no viene en ese orden retornara 0
        #conversion de la fecha a formato fecha
        final_date = datetime.strptime(date,formato_fecha)
        #condicional que evalua si la fecha es mayor a 1900/01/01
        if final_date > fecha:
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except:
        return 0