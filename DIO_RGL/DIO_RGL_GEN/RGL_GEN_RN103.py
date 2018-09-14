# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN103 (ID=103)
#GLOSA: VALIDACIÓN DEL DIGITO CERIFICADOR DEL RUT
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 06/06/2018
#AUTOR: MIGUEL CHOQUE HERRERA

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN103",UDF_RN103("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN052 = udf(RN103,StringType())

import re
from itertools import cycle

#funcion ulizada para evaluar el digito verificador a partir del rut ingresado
def RGL_GEN_RN103(Input,dv):
    try:
        if (" " in Input) == True:
            return 0
        else:
            reversed_digits = map(int, reversed(str(Input)))
            factors = cycle(range(2, 8))
            s = sum(d * f for d, f in zip(reversed_digits, factors))
            s2= (-s) % 11
            digito_verificador = 0
            try:
                if dv.upper() == "K":
                    digito_verificador = 10
                    if digito_verificador == s2 and len(Input)==9:
                        return 1
                    else:
                        return 0
                else:
                    digito_verificador = int(dv)    
                    if digito_verificador == s2 and len(Input)==9:
                        return 1
                    else:
                        return 0
            except ValueError:
                return 0
    except:
        return 0