# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_30_001
#GLOSA: FECHA VALIDA (YYYY-MM-DD)
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ


from datetime import datetime

#definicion de la funcion que evalua si Fecha válida (año-mes-día), recibe tres parametros en el siguinete orden : año,mes,dia
def CLI_30_001(year,month,day):
    #se crea un estring que contenga la fecha en formato s/y/m/d
    formato_fecha = '%Y/%m/%d'
    date = year + "/" + month + "/"+ day
    #try en caso de que no venga en el orden año-mes-dia, si no viene en ese orden retornara 0
    try:
        #conversion de la fecha a formato fecha
        #condicional que evalua si la fecha es valida o no
        if datetime.strptime(date,formato_fecha):
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except:
        return 0