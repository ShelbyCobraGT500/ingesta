# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN006 (ID=6)
#GLOSA NO CONTENGA ESPACIOS
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN006",UDF_RN006("<nombre columna en la cual se debe aplicar la regla>")).
#UDF_RN006 = udf(RN006,StringType())

import re

#Definicion: funcion que evalua si el dato NO contiene espacios.
def RGL_GEN_RN006(Input):
    #Evalua si el dato no contiene caracter blanco
    try:
        if float(Input) != 0:
            if re.search(' ',Input):
                if re.search('\t',Input):
                    if re.search('\r',Input):
                        if re.search('\n',Input) < 0:
                            if Input != '':
                                return 1 # dato no tiene ningun tipo de dato blanco
                            else:
                                return 0 # dato esta vacio
                        else:
                            return 0 # dato tiene salto de linea
                    else:
                        return 0 # dato tiene salto de carro
                else:
                    return 0 # dato tiene tab
            else:
                return 0 # dato tiene blanco
        else:
            return 0 # dato es cero
    except ValueError:
        return 0