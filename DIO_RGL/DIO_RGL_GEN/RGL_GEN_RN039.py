# encoding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN039 (ID=39)
#GLOSA: VALIDAR SI DATO NUMERICO ES ENTERO ENTRE 0 A 999.999.999
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN039",UDF_RN039("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN039 = udf(RN039,StringType())

#Definición de la funcion que evalua si el dato es entero entre 0 a 999 millones
def RGL_GEN_RN039(Input):
    #Validacion si el numero tiene solo numeros entre 0 o 9
    if Input.strip(" ").isdigit():
        if int(Input) >= 0:
            if int(Input) <= 999999999:
                return 1
            else:
                return 0
        else:
            return 0
    else:
        return 0