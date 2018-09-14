# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN004 (ID=4)
#GLOSA: NO CONTENGA ESPACIOS
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 31/05/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN004",udf_RN004(<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN004 = udf(RN004,StringType())

import re

#Definicion: funcion que evalua si el dato NO contiene espacios.
def RGL_GEN_RN004(Input):
    #Evalua si el dato no contiene caracter blanco
    try:
        if re.search(' ',Input):
            if re.search('\t',Input):
                if re.search('\r',Input):
                    if re.search('\n',Input):
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
    except ValueError:
        return 0