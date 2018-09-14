--
--

[cloudera@quickstart ~]$ hadoop fs -ls /user/
[cloudera@quickstart ~]$ hadoop fs -put /home/cloudera/T133OPC_limpio.txt /user/cloudera/
[cloudera@quickstart ~]$ spark-shell

--
--
// IMPORTAMOS CLASES NECESARIAS

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row


// HDFS A RDD

val rdd = sc.textFile("hdfs:///user/spark/OPC_F201803.TXT")
# 5s en leer un hdfs de 2gb, con 2gb de ram

// DESDE ARCHIVO PLANO A RDD

// val rdd_raw = scala.io.Source.fromFile("/home/cloudera/T133OPC_limpio.txt").getLines.toList // SE QUEDA SIN MEMORIA
// val rdd = sc.parallelize(rdd_raw)

// RDD A DATA FRAME+ESQUEMA

// crear schema
// val schema = StructType( ...

// creamos la funcion especifica en caso que el archivo de entrada sea secuencial de largo fijo, en vez de separado por caracteres de largos variables

def getRow(x : String) : Row={
val columnArray = new Array[String](95)
columnArray(0)=x.substring(0,10)
columnArray(1)=x.substring(11,14)
columnArray(2)=x.substring(15,17)
columnArray(3)=x.substring(18,20)
columnArray(4)=x.substring(21,23)
columnArray(5)=x.substring(24,26)
columnArray(6)=x.substring(27,29)
columnArray(7)=x.substring(30,44)
columnArray(8)=x.substring(45,58)
columnArray(9)=x.substring(59,60)
columnArray(10)=x.substring(61,62)
columnArray(11)=x.substring(63,64)
columnArray(12)=x.substring(65,66)
columnArray(13)=x.substring(67,68)
columnArray(14)=x.substring(69,70)
columnArray(15)=x.substring(71,72)
columnArray(16)=x.substring(73,74)
columnArray(17)=x.substring(75,76)
columnArray(18)=x.substring(77,78)
columnArray(19)=x.substring(79,80)
columnArray(20)=x.substring(81,82)
columnArray(21)=x.substring(83,84)
columnArray(22)=x.substring(85,86)
columnArray(23)=x.substring(87,88)
columnArray(24)=x.substring(89,90)
columnArray(25)=x.substring(91,91)
columnArray(26)=x.substring(92,93)
columnArray(27)=x.substring(94,94)
columnArray(28)=x.substring(95,103)
columnArray(29)=x.substring(104,106)
columnArray(30)=x.substring(107,107)
columnArray(31)=x.substring(108,113)
columnArray(32)=x.substring(114,119)
columnArray(33)=x.substring(120,120)
columnArray(34)=x.substring(121,129)
columnArray(35)=x.substring(130,135)
columnArray(36)=x.substring(136,136)
columnArray(37)=x.substring(137,148)
columnArray(38)=x.substring(149,151)
columnArray(39)=x.substring(152,157)
columnArray(40)=x.substring(158,158)
columnArray(41)=x.substring(159,159)
columnArray(42)=x.substring(160,161)
columnArray(43)=x.substring(162,163)
columnArray(44)=x.substring(164,165)
columnArray(45)=x.substring(166,167)
columnArray(46)=x.substring(168,169)
columnArray(47)=x.substring(170,171)
columnArray(48)=x.substring(172,173)
columnArray(49)=x.substring(174,175)
columnArray(50)=x.substring(176,176)
columnArray(51)=x.substring(177,179)
columnArray(52)=x.substring(180,196)
columnArray(53)=x.substring(197,200)
columnArray(54)=x.substring(201,202)
columnArray(55)=x.substring(203,203)
columnArray(56)=x.substring(204,205)
columnArray(57)=x.substring(206,207)
columnArray(58)=x.substring(208,209)
columnArray(59)=x.substring(210,211)
columnArray(60)=x.substring(212,213)
columnArray(61)=x.substring(214,215)
columnArray(62)=x.substring(216,217)
columnArray(63)=x.substring(218,219)
columnArray(64)=x.substring(220,220)
columnArray(65)=x.substring(221,223)
columnArray(66)=x.substring(224,231)
columnArray(67)=x.substring(232,232)
columnArray(68)=x.substring(233,234)
columnArray(69)=x.substring(235,237)
columnArray(70)=x.substring(238,245)
columnArray(71)=x.substring(246,253)
columnArray(72)=x.substring(254,259)
columnArray(73)=x.substring(260,263)
columnArray(74)=x.substring(264,273)
columnArray(75)=x.substring(274,274)
columnArray(76)=x.substring(275,278)
columnArray(77)=x.substring(279,281)
columnArray(78)=x.substring(282,289)
columnArray(79)=x.substring(290,295)
columnArray(80)=x.substring(296,309)
columnArray(81)=x.substring(310,316)
columnArray(82)=x.substring(317,330)
columnArray(83)=x.substring(331,344)
columnArray(84)=x.substring(345,345)
columnArray(85)=x.substring(346,360)
columnArray(86)=x.substring(361,375)
columnArray(87)=x.substring(376,390)
columnArray(88)=x.substring(391,405)
columnArray(89)=x.substring(406,420)
columnArray(90)=x.substring(421,435)
columnArray(91)=x.substring(436,450)
columnArray(92)=x.substring(451,465)
columnArray(93)=x.substring(466,480)
columnArray(94)=x.substring(481,483)
Row.fromSeq(columnArray)
}

// creamos el dataframe desde el archivo en hdfs, usando mapeo y una funcion anonima junto con el esquema

val df = sqlContext.createDataFrame(sc.textFile("/user/spark/OPC_F201803.TXT").map { x => getRow(x) }, schema)

// le agregamos una columna nueva de valor fijo 

val df_new = df.withColumn("rn001", lit(0))

// creamos una funcion personalizada con la validacion rn001 el dato no puede venir nulo o vacio

def rn001(x: String): String = 
x.trim match {
  case isEmpty => "0"
  case isNull => "0"
  case "NULL" => "0"
  case _ => "1"
}	


// CODIGO DE LA REGLA DE CALIDAD: RN004
// GLOSA: Numerico de largo 9
// COMPLEJIDAD: BAJA (Aplica a 7 EDC)
// PROBADO EN SCALA SPARK util.Properties.versionNumberString=2.10.5
// EJEMPLO DE COMO APLICAR LA FUNCION
// val df_new = df.withColumn("RN001_EDC", rn001_completitud_nulos_o_vacio($"NOMBRE DE LA COLUMNA"))

val rn004_numerico_de_largo_x = udf(
(x: String, largo: Int) =>
  x.matches("[-+]?\\d+(\\.\\d+)?") == true && x.length == largo match {
    case true => "1"
    case false => "0"
  }
)

//SQL
// Para correr un SQL primero debemos registrar una tabla en base al data frame
// Se puede generar mas de un registro de tabla para consultas complejas con joins
df_new.registerTempTable("tb_rn004")

//Generamos consulta apuntando al nombre con el que registramos la tabla, .show puede recibir parametros 
//para limitar la cantidad de resultados

sqlContext.sql("SELECT COUNT(*)AS EXITO  FROM TB_RN004 WHERE RN004_EDC=1").show()

//Para almacenar la salida de la consulta en otro DF debemos correr:
val dfrn004 = sqlContext.sql("SELECT COUNT(*)AS EXITO  FROM TB_RN004 WHERE RN004_EDC=1")

dfrn004.registerTempTable("TB_RN004")

dfrn004.show
