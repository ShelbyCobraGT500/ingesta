# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN050 (ID=050)
#GLOSA: VALIDA SI LA FECHA ES MENOR O IGUAL A LA FECHA DE CREACION DEL ARCHIVO ANALIZADO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 06/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN050",UDF_RN050("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN050 = udf(RN050,StringType())

import datetime
from datetime import datetime
from datetime import date

#definición de la funcion que evalua si la fecha de ejecucion es mayor o menor a la ejecucion del archivo
def RGL_GEN_RN050(century,year,month,day,aniocre,monthcre,daycre):
    #Creacion de la fecha de creación a partir de 3 string que indican año mes dia
    try:
        fecha_creacion = aniocre + "/" + monthcre + "/" + daycre
        fecha_creacion_date = datetime.strptime(fecha_creacion,'%Y/%m/%d')
        #Creacion de la fecha a partir de 4 strings
        fecha = century + year + "/" + month + "/" + day
        fecha_date = datetime.strptime(fecha,'%Y/%m/%d')
        #Calculamos las diferencias de las fechas
        diferencia_fechas = fecha_creacion_date - fecha_date
        diferencia_fechas_dias = diferencia_fechas.days
        #condicional para evaluar la diferencia es mayor o igual a 0
        if diferencia_fechas_dias >= 0:
            return 1
        else:
            return 0
    except:
        return 0 
#rn050("20","15","01","24","2018","03","01")
    
