"""Este archivo corresponde al catalogo de reglas programadas en scala a partir del archivo
Catalogo RC Genericas al 03-04.xlsx
El objetivo es generar un catalogo de funciones las cuales luego seran llamadas por los script
que procesan cada uno de los archivos mediante el uso de packages o paquetes.-"""

#INICIO DE CATALOGO

#CODIGO DE LA REGLA DE CALIDAD: RN001
#GLOSA: Comprende las siguientes reglas
#   RN003 (ID=3) = No puede venir en blanco
#   RN001 (ID=1) = No debe tener nulos
#   RN002 (ID=2) = No debe venir en blanco/nulos
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn001", udf_rn001_completitud_sin_nulos_o_vacios("<nombre columna en la cual se debe aplicar la regla>"))

#definicion de la funcion
def rn001_completitud_sin_nulos_o_vacios (Input):
  #condicional que evalua si cumple con los parametros establecidos, transforma el dato de Unicode a Ascii
  if not Input.encode('ascii','ignore').strip() or Input.encode ('ascii','ignore').strip() == "NULL":
    return 0 # si esta vacio "", blanco ","nulo u('NULL')"
  else:
    return 1 # en caso contrario

#definicion de UDF (User Define Function) para aplicar la regla al data frame
udf_rn001_completitud_sin_nulos_o_vacios = udf(rn001_completitud_sin_nulos_o_vacios, StringType())



#CODIGO DE LA REGLA DE CALIDAD:RN036 (ID=36)
#GLOSA: El numero debe ser mayor o igual a 0
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn036",udf_rn036_mayor_o_igual_0(<nombre columna en la cual se debe aplicar la regla>"))

#funcion que evalua si el numero es >= a 0
def rn036_mayor_o_igual_0(Input):
    #Evalua si el caracter trae alguna letra o simbolo si es asi pasara al else
    try:
        num = float(Input)
        if num >= 0:
            return 1 # en caso de ser mayor o igual a cer
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0

#definicion de UDF (User Define Function) para aplicar la regla al data frame
udf_rn036_mayor_o_igual_0 = udf(rn036_mayor_o_igual_0, StringType())



#CODIGO DE LA REGLA DE CALIDAD:RN007 (ID=7)
#GLOSA: Fecha distinta de 1900/01/01
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn007",udf_rn007_distinto_a_1900_01_01(<nombre de las columnas en la cual se debe aplicar la regla deben ir en formato año,mes,dia>")) 

#definicion de la funcion que evalua si la fecha es mayor a 1900/01/01, recibe tres parametros en el siguinete orden : año,mes,dia
# from datetime import datetime

def rn007_distinto_a_1900_01_01(century,year,month,day):
    formato_fecha = '%Y/%m/%d'
    fecha = datetime.strptime('1900/01/01',formato_fecha)
    #se crea un estring que contenga la fecha en formato s/y/m/d
    newcentury = int(century) - 1
    date = str(newcentury) + year + "/" + month + "/"+ day
    #try en caso de que no venga en el orden año-mes-dia, si no viene en ese orden retornara 0
    try:
        #conversion de la fecha a formato fecha
        final_date = datetime.strptime(date,formato_fecha)
        #condicional que evalua si la fecha es mayor a 1900/01/01
        if final_date > fecha:
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except ValueError:
        return 0

#definicion de UDF (User Define Function) para aplicar la regla al data frame
udf_rn007_distinto_a_1900_01_01 = udf(rn007_distinto_a_1900_01_01,StringType())



#CODIGO DE LA REGLA DE CALIDAD:RN009
#ABARCA REGLAS:
#   rn_009 (ID=9)  = Numerico de largo 1
#   rn_010 (ID=10) = Numerico de largo 2
#   rn_011 (ID=11) = Numerico de largo 3
#   rn_017 (ID=17) = Numerido de largo 15
#   rn_012 (ID=12) = Numerico de largo 4
#   rn_016 (ID=16) = Numerico de largo 11
#   rn_013 (ID=13) = Numerico de largo 5
#   rn_014 (ID=14) = Numerico de largo 9
#   rn_015 (ID=15) = Numerico de largo 10
#GLOSA: Evalua que una variable numerica posea un largo x(definido por el usuario)
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn009",rn009_valor_num_tam_x(<nombre de las columnas en la cual se debe aplicar la regla>,x(donde x es el tamaño a evaluar)))

def rn009_valor_num_tam_x(Input,size):
    try:
        In = int(Input)
        tam = int(size)
        if len(str(In)) == tam:
            return 1
        else:
            return 0
    except ValueError:  
        return 0

#definicion de UDF (User Define Function) para aplicar la regla al data frame
udf_rn009_valor_num_tam_x = udf(rn009_valor_num_tam_x,StringType())



#------------------------------------------------------------------------------------------------------
#CODIGO DE LA REGLA DE CALIDAD:RN019 (ID=19)
#Glosa: Dato No debe contener decimales.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn0019",udf_rn019_No_debe_tener_decimales(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato no contiene decimales
def rn019_No_debe_tener_decimales(Input):
#Evalua si el dato NO tiene decimales
    try:
        if Input.isdigit() or int(Input):
            return 1 # si el dato es digito y es entero
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN029 (ID=29)
#Glosa: Dato debe ser distinto a 0.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn029",udf_rn031_debe_ser_diferente_00(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato no contiene decimales
def rn029_debe_ser_diferente_0(Input):
    #Evalua si el dato es diferente a '0'
    try:
        if Input != '0' and len(Input) == 1:
            return 1 # si el dato es distinto a '0'
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN030 (ID=30)
#Glosa: Dato debe ser distinto a 00.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn030",udf_rn031_debe_ser_diferente_00(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato no contiene decimales
def rn030_debe_ser_diferente_00(Input):
    #Evalua si el dato es diferente a '00'
    try:
        if Input != '00' and len(Input) == 2:
            return 1 # si el dato es distinto a '00'
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN031 (ID=31)
#Glosa: Dato debe ser distinto a 000.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn013",udf_rn031_debe_ser_diferente_000(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato no contiene decimales
def rn031_debe_ser_diferente_000(Input):
    #Evalua si el dato es diferente a '000'
    try:
        if Input != '000' and len(Input) == 3:
            return 1 # si el dato es distinto de '000'
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN018 (ID=18)
#Glosa: Dato debe ser numerico.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn018",udf_rn018_debe_ser_numerico(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato es numerico
def rn018_debe_ser_numerico(Input):
    #Evalua si el dato es numerico
    try:
        if Input.isdigit() or float(Input):
            return 1 # si el dato es numerico
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0





#---------------------------------------------------------------------------------------------
#CODIGO DE LA REGLA DE CALIDAD:RN037 (ID=37)
#Glosa: Debe ser mayor a cero.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn037",udf_rn037_dato_mayor_a_cero(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato es mayor a cero
def rn037_dato_mayor_a_cero(Input):
    #Evalua si el dato es mayor a cero
    try:
        if float(Input) > 0:
            return 1 # si el dato es mayor a cero
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0


# coding=utf-8
#CODIGO DE LA REGLA DE CALIDAD:RN004 (ID=4)
#Glosa: No contenga espacios.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn004",rn004_dato_NO_contiene_blanco(<nombre columna en la cual se debe aplicar la regla>"))
import re

#Definicion: funcion que evalua si el dato NO contiene espacios.
def rn004_dato_NO_contiene_blanco(Input):
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



udf_rn004_dato_NO_contiene_blanco = udf(rn004_dato_NO_contiene_blanco,StringType())

#CODIGO DE LA REGLA DE CALIDAD:RN006 (ID=6)
#Glosa: No contenga es
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn006",udf_rn006_dato_mayor_a_cero(<nombre columna en la cual se debe aplicar la regla>")).
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#Definicion: funcion que evalua si el dato NO contiene espacios.
def rn006_dato_NO_contiene_blanco_o_solo_0(Input):
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


#CODIGO DE LA REGLA DE CALIDAD:RN022 (ID=22)
#Glosa: Debe ser mayor o igual a uno.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn022",udf_rn022_dato_mayor_igual_a_cero(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato es mayor a igual a uno
def rn022_dato_mayor_igual_a_uno(Input):
    #Evalua si el dato es mayor igual a uno
    try:
        if float(Input) >= 1:
            return 1 # si el dato es mayor o igual a uno
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN020 (ID=20)
#Glosa: Dato debe ser un porcentaje valido
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn020",udf_rn020_debe_porcentaje_valido(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si es un porcentaje valido
def rn020_debe_porcentaje_valido(Input):
    #try en caso de que se presente un error
    try:
        #validacion si el porcentaje usado es valido
        if float(Input) > 0 and float(Input):
            return 1
        else:
            return 0
    except ValueError:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN026 (ID=26)
#Glosa: Dato debe ser alfanumerico de largo 5
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn026",udf_rn026_alfanumerico_largo_5(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 5
def rn026_alfanumerico_largo_5(Input):
    #validacion si el caracter es alfanumerico y de largo 5
    if Input.isalnum() and len(Input)==5:
        return 1
    else:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN025 (ID=25)
#Glosa: Dato debe ser alfanumerico de largo 4
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn025",udf_rn025_alfanumerico_largo_4(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 4
def udf_rn025_alfanumerico_largo_4(Input):
    #validacion si el caracter es alfanumerico y de largo 5
    if Input.isalnum() and len(Input)==4:
        return 1
    else:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN027 (ID=27)
#Glosa: Dato debe ser largo obligatorio 8
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn027",udf_rn027_largo_de_8(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el Input es de largo 8
def rn027_largo_de_8(Input):
    #validacion si el caracter es alfanumerico y de largo 8
    if len(Input)==8:
        return 1
    else:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN033 (ID=33)
#Glosa: Dato debe ser F o M
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn033",udf_rn033_F_o_M(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el caracter es F o M
def rn033_F_o_M(Input):
    #validacion si el caracter es F o M
    if Input == "F" or Input == "M":
        return 1
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN043 (ID=43)
#Glosa: Valor entre [0-9]
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn043",udf_rn043_0_9(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor esta entre 0 y 9
def rn043_0_9(Input):
    try:
        #validacion si el valor esta entre 0 o 9
        if 0 <= float(Input) <= 9:
            return 1
        else:
            return 0
    except ValueError:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN040 (ID=40)
#Glosa: Codigo debe ser 01, 03 o 08
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn040",udf_rn040_0103o08(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el codigo es 03, 01 o 08
def rn040_0103o08(Input):
    #validacion si el valor esta 01, 03 o 08
    if Input == "03" or Input == "01" or Input == "08":
        return 1
    else:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN124 (ID=124)
#Glosa: Codigo debe ser 1 o 2
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn124",udf_rn124_1o2(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor es 1 o 2
def rn124_1o2(Input):
    #validacion si el valor es 1 o 2
    if Input == "1" or Input == "2":
        return 1
    else:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN125 (ID=125)
#Glosa: Codigo debe ser 1,3 o 4
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn125",udf_rn125_1_3_o_4(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor es 1, 3 o 4
def rn125_1_3_o_4(Input):
    #validacion si el valor es 1, 3 o 4
    if Input == "1" or Input == "3" or Input == "3":
        return 1
    else:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN132 (ID=132)
#Glosa: Rut largo 9 y dv entre 0 y 9 o k
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn132",udf_rn132_rut_largo_9_dv_0_9_o_k(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor es 1, 3 o 4
def rn132_rut_largo_9_dv_0_9_o_k(Input,dv):
    #validacion si rut es de largo 9 y el dv es entre 0 o 9 o k
    if  len(Input)==9 and Input.isdigit():
        if '0' <= dv <= '9' or dv is 'k':
            return 1
        else:
            return 0
    else:
        return 0

# encoding=utf-8
import re
from itertools import cycle



#CODIGO DE LA REGLA DE CALIDAD:RN127 (ID=127)
#Glosa: Validación de correo electronico
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn127",udf_rn127_validar_mail(<nombre columna en la cual se debe aplicar la regla>"))

#Función utilizada para validar el correo en la cual se utiliza un re.match para revisar si el correo es valido segun el estadar
def rn127_validar_mail(Input):
    if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)",Input):
        return 1
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN103 (ID=103)
#Glosa: Validación del digito verificador del rut
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn103",udf_rn103_validar_dv(<nombre columna en la cual se debe aplicar la regla>"))

#funcion ulizada para evaluar el digito verificador a partir del rut ingresado
def rn103_validar_dv(Input,dv):
    reversed_digits = map(int, reversed(str(Input)))
    factors = cycle(range(2, 8))
    s = sum(d * f for d, f in zip(reversed_digits, factors))
    s2= (-s) % 11
    digito_verificador = 0
    try:
        if dv.upper() == "K":
            digito_verificador = 10
            if digito_verificador == s2:
                return 1
            else:
                return 0
        else:
            digito_verificador = int(dv)    
            if digito_verificador == s2:
                return 1
            else:
                return 0
    except ValueError:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN034 (ID=34)
#Glosa: Validar si Dato numerico es 1 , 2, 3, 4 o 9
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn034",udf_rn034_validar_N12349(<nombre columna en la cual se debe aplicar la regla>"))

#deficion que evalua si el dato numeri es 1,2,3,4 o 9
def rn034_validar_N12349(Input):
    if Input == "1" or Input == "2" or Input == "4" or Input == "9":
        return 1
    else:
        return 0

#CODIGO DE LA REGLA DE CALIDAD:RN039 (ID=39)
#Glosa: Validar si Dato numerico es entero entre 0 - 999.999.999
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn039",udf_rn039_entero_entre_0_a_999M(<nombre columna en la cual se debe aplicar la regla>"))

#Definición de la funcion que evalua si el dato es entero entre 0 a 999 millones
def rn039_entero_entre_0_a_999M(Input):
    #Validacion si el numero tiene solo numeros entre 0 o 9
    if Input.isdigit():
        Input2 = int(Input)
        if Input2 >= 0:
            if Input2 <= 999999999:
                return 1
            else:
                return 0
        else:
            return 0
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN024 (ID=24)
#Glosa: Dato debe ser alfanumerico de largo 3
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn024",udf_rn024_alfanumerico_largo_3(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 3
def rn024_alfanumerico_largo_3(Input):
    #validacion si el caracter es alfanumerico y de largo 3
    if Input.isalnum() and len(Input)==3:
        return 1
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN005 (ID=05)
#Glosa: Dato no debe conntener ni puntos ni guiones
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn005",udf_rn005_ni_puntos_ni_guiones(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba que la cadena no tenga ni puntos ni caracteres
def rn005_ni_puntos_ni_guiones(Input):
    bandera = 0
    #ciclo que recorre el caracter hasta encontrar un . o un -
    for caract in Input:
        if caract == '.' or caract == '-':
            bandera = 1
            break
    #condicional que evalua si el Input posee . o - dependiendo de el valor de bandera en el caso de bandera es 1 quiere decir que si tiene puntos o -
    if bandera == 1:
        return 0
    else:
        return 1



#CODIGO DE LA REGLA DE CALIDAD:RN033 (ID=35)
#Glosa: Dato debe ser S o N
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn035",udf_rn033_S_o_N(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el caracter es S o N
def rn035_S_o_N(Input):
    #validacion si el caracter es S o N
    if Input.upper() == "S" or Input.upper() == "N":
        return 1
    else:
        return 0


#-------------------------------------------------------------------------------------
#CODIGO DE LA REGLA DE CALIDAD:RN038 (ID=38)
#Glosa: Validar si Dato numerico es 1 o 2 o 3 o 9
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn038",rn038_validar_entero_1234(<nombre columna en la cual se debe aplicar la regla>"))

#deficion que evalua si el dato numeri es 1 o 2 o 3 o 9
def rn038_validar_entero_1239(Input):
    try:
        dato= int(Input)
        if dato == 1 or dato == 2 or dato == 3 or dato == 9:
            return 1
        else:
            return 0
    except:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN032 (ID=32)
#Glosa: Debe ser mayor a 00 y menor que 100 (0 < dato < 100)
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn032",udf_rn032_dato_mayor_a_cero_y_menor_a_100(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion: funcion que evalua si el dato es mayor a cero y menor a cien
def rn032_dato_mayor_a_cero_y_menor_a_100(Input):
    #Evalua si el dato es mayor a cero y menor a cien
    try:
        if float(Input) > 0 and float(Input) < 100:
            return 1 # si el dato es mayor a cero y menor a cien
        else:
            return 0 #en caso contrario
    except ValueError:
        return 0

        
#CODIGO DE LA REGLA DE CALIDAD:RN028 (ID=28)
#Glosa: Dato debe ser expresado en carcateres
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn024",udf_rn028_dato_expresado_en_caracteres(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el caracter es alfanumerico de largo 3
def rn028_dato_expresado_en_caracteres(Input):
    #validacion si el caracter es alfanumerico y de largo 3
    if Input.isalnum():
        return 1
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN021 (ID=21)
#Glosa: Validar que el dato sea entero.
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn038",rn021_solo_dato_entero(<nombre columna en la cual se debe aplicar la regla>"))

#deficion que evalua si el dato numeri es 1 o 2 o 3 o 9
def rn021_solo_dato_entero(Input):
    try:
        if int(Input) or Input == '0':
            return 1
        else:
            return 0
    except:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN023 (ID=23)
#Glosa: Largo minimo de un carácter
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn023",udf_rn023_largo_minimo_un_caracter(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el gargo del dato es de minimo un caracter alfanumerico
def rn023_largo_minimo_un_caracter(Input):
    #validacion largo minimo es de un carácter alfanumerico
    try:
        if Input.isalnum() and len(Input)>=1:
            return 1
        else:
            return 0
    except:
        return 0



#CODIGO DE LA REGLA DE CALIDAD:RN008 (ID=8)
#GLOSA: Fecha válida (año-mes-día)
#COMPLEJIDAD: BAJA (porque aplica a solo un EDC)
#PROBADO EN Pyspark
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn008",rn008_fecha_valida_yyyy_mm_dd(<nombre de las columnas en la cual se debe aplicar la regla deben ir en formato año,mes,dia>")) 

#definicion de la funcion que evalua si Fecha válida (año-mes-día), recibe tres parametros en el siguinete orden : año,mes,dia
# from datetime import datetime
def rn008_fecha_valida_yyyy_mm_dd(century,year,month,day):
    #se crea un estring que contenga la fecha en formato s/y/m/d
    newcentury = int(century) - 1
    formato_fecha = '%Y/%m/%d'
    date = str(newcentury) + year + "/" + month + "/"+ day
    print(date)
    #try en caso de que no venga en el orden año-mes-dia, si no viene en ese orden retornara 0
    try:
        #conversion de la fecha a formato fecha
        #condicional que evalua si la fecha es valida o no
        if datetime.strptime(date,formato_fecha):
            return 1 #en caso que la fecha sea mayor
        else:
            return 0 # en caso que la fecha sea menor 
    except ValueError:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN044 (ID=44)
#Glosa: RUT de banco extranjero es valido si el dato es un rut mayor a 40 millones
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn044",udf_rn044_RUT_MAY_A_40_MILLONES(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor es mayor a 40 millones
def rn044_RUT_MAY_A_40_MILLONES(Input):
    #validacion si el dato es digito
    if  Input.isdigit():
        #validación si el dato es un entero >= 40.000.000
        if int(Input) >= 40000000:
            return 1
        else:
            return 0
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN047 (ID=47)
#Glosa: RUT (Sinacofi) es válido si el dato esta entre 20.050.000 y 30 millones (se asume rut sin dv)
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn047",udf_rn047_RUT_ENTRE_20_Y_30_MILLONES(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor es mayor a 40 millones
def rn047_RUT_ENTRE_20_Y_30_MILLONES(Input):
    #validacion si rut es de largo 9 y el dv es entre 0 o 9 o k
    if  Input.isdigit():
        if int(Input) >= 20000000 and int(Input) =< 30000000:
            return 1
        else:
            return 0
    else:
        return 0


#CODIGO DE LA REGLA DE CALIDAD:RN053 (ID=53)
#Glosa: Dato es valido si no contiene valor mayor a 100
#COMPLEJIDAD: BAJA (porque aplica a solo un Elemento Dato Critico)
#PROBADO EN Python 3.5.2
#EJEMPLO DE COMO APLICAR LA FUNCION
#df_final = df_new.withColumn("rn053",udf_rn053_DTO_NO_MAY_A_100(<nombre columna en la cual se debe aplicar la regla>"))

#Definicion de la funcion que comprueba si el valor es mayor a 40 millones
def rn053_DTO_NO_MAY_A_100(Input):
    #validacion si rut es de largo 9 y el dv es entre 0 o 9 o k
    if  Input.isdigit():
        if float(Input) <= 100:
            return 1
        else:
            return 0
    else:
        return 0