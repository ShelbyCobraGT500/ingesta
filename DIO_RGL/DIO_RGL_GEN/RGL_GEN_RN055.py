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

def RGL_GEN_RN055(Input, Creacion):
    try:
        fecha_cre_date = datetime.strptime(Creacion,'%Y%m%d')
        fecha_nac_date = datetime.strptime(Input,'%Y%m%d')
        diferencia_fechas = fecha_cre_date - fecha_nac_date
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