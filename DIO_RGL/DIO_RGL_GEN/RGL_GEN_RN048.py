#coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN048 (ID=048)
#GLOSA: VALIDA SI LA EDAD DEL CLIENTE ES MAYOR A 18 AÑOS EN LA FECHA DE CREACIÓN DEL ARCHIVO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 06/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN048",UDF_RN048("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN048 = udf(RN048,StringType())

import datetime
from datetime import datetime
from datetime import date

#definición de la funcion que evalua si la persona es mayor a 18 años a partir de la fecha de creacion del archivo evaluado
def RGL_GEN_RN048(century,year,month,day,aniocre,monthcre,daycre):
    #Creacion de la fecha de creación a partir de 3 string que indican año mes dia
    try:
        fecha_creacion = aniocre + "/" + monthcre + "/" + daycre
        fecha_creacion_date = datetime.strptime(fecha_creacion,'%Y/%m/%d')
        #Creacion de la fecha de nacimiento a partir de 4 strings
        fecha_nac = century + year + "/" + month + "/" + day
        fecha_nac_date = datetime.strptime(fecha_nac,'%Y/%m/%d')
        #Calculamos las diferencias de las fechas
        diferencia_fechas = fecha_creacion_date - fecha_nac_date
        diferencia_fechas_dias = diferencia_fechas.days
        #transformamos a edad numerica
        edad_numerica = diferencia_fechas_dias / 365.2425
        edad = int(edad_numerica)
        #print (edad)
        #condicional para evaluar si la edad es mayor de 18 años
        if edad >= 18:
            return 1
        else:
            return 0
    except:
        return 0        

#rn048("20","15","01","24","2018","03","01")
#primeros 4 parametros corresponden a lo que lee desde el archivo, el resto se indica de forma manual e 
#indica la fecha en que se creo el archivo recibido    
