# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:TG_1_006 ()
#GLOSA: Pais distinto a 160
#COMPLEJIDAD: BAJA
#PROBADO EN Pyspark
#FECHA MODIFICACION: 20/06/2018
#AUTOR: FELIPE AVALOS


#Definicion: funcion que evalua si el dato no contiene decimales
def TG_1_006(Input):
    #Evalua si el dato es distinto a 160
    try:
        if int(Input) <> 160:
            return 1
        else:
            return 0
    except:
        return 0