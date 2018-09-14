# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN127 (ID=127)
#GLOSA: VALIDACIÓN DEL CORREO ELECTRONICO
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 23/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN127",UDF_RN127("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN127 = udf(RN127,StringType())

import re
from itertools import cycle

#Función utilizada para validar el correo en la cual se utiliza un re.match para revisar si el correo es valido segun el estadar
def RGL_GEN_RN127(Input):
    try:
        if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)",Input.strip()):
            return 1
        else:
            return 0
    except:
        return 0