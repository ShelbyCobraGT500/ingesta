# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN036 (ID=36)
#GLOSA: EL NUMERO DEBE SER MAYOR O IGUAL A 0
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 04/06/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

#CREACIÓN UDF PARA LLAMAR A FUNCIÓN EN PYSAPRK
#df_final = df_new.withColumn("RN036",UDF_RN036("<nombre columna en la cual se debe aplicar la regla>"))
#UDF_RN036 = udf(RN036,StringType())

#funcion que evalua si el numero es >= a 0
def RGL_GEN_RN036(Input):
    #Evalua si el caracter trae alguna letra o simbolo si es asi pasara al else
    try:
        num = float(Input)
        if num >= 0:
            return 1 # en caso de ser mayor o igual a cer
        else:
            return 0 #en caso contrario
    except:
        return 0