# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN044 (ID=44)
#GLOSA: RUT DE BANCO EXTRANJERO ES VALIDO SI EL DATO ES UN RUT MAYOR O IGUAL A 40 MILLONES
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 07/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN044",UDF_RN044("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN044 = udf(RN044,StringType())

#Definicion de la funcion que comprueba si el valor es mayor a 40 millones
def RGL_GEN_RN044(Input):
    #validacion si el dato es digito
    if  Input.isdigit():
        #validación si el dato es un entero >= 40.000.000
        if int(Input) >= 40000000:
            return 1
        else:
            return 0
    else:
        return 0