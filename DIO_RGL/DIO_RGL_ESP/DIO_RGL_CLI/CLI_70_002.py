# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_70_002 
#GLOSA: FECHA DISTINTA A 1900/01/01
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 09/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ


from datetime import datetime

#DEFINICIÓN DE LA FUNCION QUE EVALUA SI LA FECHA ES DISTINTA DE 1900/01/01
def CLI_70_002(year,month,day):
    formato_fecha = '%Y/%m/%d'
    fecha = datetime.strptime('1900/01/01',formato_fecha)
    date = year + "/" + month + "/"+ day
    #try en caso de que no venga en el orden año-mes-dia, si no viene en ese orden retornara 0
    try:
        #conversion de la fecha a formato fecha
        final_date = datetime.strptime(date,formato_fecha)
        #condicional que evalua si la fecha es mayor a 1900/01/01
        if final_date > fecha:
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except:
        return 0