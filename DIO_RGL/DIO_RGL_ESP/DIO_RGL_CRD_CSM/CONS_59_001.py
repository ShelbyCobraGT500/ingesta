# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:CONS_59_001
#GLOSA: VALORES 0,1,2,4,6,7,9
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#FECHA MODIFICACION: 08/08/2018
#AUTOR: MIGUEL CHOQUE HERRERA

def CONS_59_001(Input):
    try:
        if Input in ("0","1","2","4","6","7","9"):
            return 1
        else:
            return 0
    except:
        return 0