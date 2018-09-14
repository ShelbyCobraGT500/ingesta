# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD: CLI_69_004
#GLOSA: VALOR NO MAYOR A 100%
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#FECHA MODIFICACION: 10/07/2018
#AUTOR: FELIPE ÁVALOS DÍAZ

def CLI_69_004(Input):
    Input_2 = Input[1]+Input[2]+Input[3]+Input[4]+Input[5]+Input[6]
    if Input_2 <= "100.00":
        return 1
    else:
        return 0
