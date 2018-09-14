# encoding=utf-8
# EJEMPLO DE QUERY RESUMEN PARA LA T103B21 SOLO CON UNA REGLA APLICADA
# REEMPLAZAR T103B21 CON CUALQUIER OTRA INTERFACE
# AGREGAR ESTE CODIGO AL FINAL DE CADA SCRIPT DE EJECUCION DE INTERFACE

# TOMAMOS EL DF CON TODAS LAS REGLAS YA APLICADAS
DF_T103LOC_PCS = 

# GENERAMOS UNA VISTA TEMPORAL PARA EJECUTAR LAS QUERYS SOBRE LA TABLA RESUMEN
DF_T103LOC_PCS.createOrReplaceTempView("RSM_T103LOC")

# CREAMOS UNA TABLA TEMPORAL CON SOLO LOS CAMPOS DE REGLAS

# agregar todas las columnas de reglas dentro del select, una tras otra, ojalá en el mismo orden de la planilla
DF_RESUMEN = spark.sql("select `RGL_GEN_RN030_TG_5`, `RGL_GEN_RN003_TG_5`, `RGL_GEN_RN013_TG_5`, `RGL_BTG_RN002` frm RSM_T103LOC")

# AHORA EL DF_RESUMEN LO PASAMOS A UNA VISTA TEMPORAL PARA EJECUTAR LAS QUERYS EN ELLA
DF_RESUMEN.createOrReplaceTempView("RSM_T130LOC_FNL")

# CADA SELECT CORRESPONDE A UNA REGLA, CAMBIAR COD_EDC, ID_REGLA, CASE Y WHERE POR CADA REGLA

#Nota MC: Para que me funcionara tuve que reemplazar las comillas por triples " -> """
DF_RSM_T130LOC = spark.sql(" \
select 'Conceptos Generales' as Dominio, \
'Código de localidades' as Cod_Loc, \
'20180613' as Periodo, \
'RGL_GEN_RN030' as Id_Regla, \
COUNT(1) TotalRegistros, \
sum(case when RGL_GEN_RN030_TG_5 = 1 then '1' else '0' end) as TotalAprobados \
from RSM_T130LOC_FNL \
where RGL_GEN_RN030_TG_5 is not null \
union all \
select 'Conceptos Generales' as Dominio, \
'Código de localidades' as Cod_Loc, \
'20180613' as Periodo, \
'RGL_GEN_RN003' as Id_Regla, \
COUNT(1) TotalRegistros, \
sum(case when RGL_GEN_RN003_TG_5 = 1 then '1' else '0' end) as TotalAprobados \
from RSM_T130LOC_FNL \
where RGL_GEN_RN003_TG_5 is not null \
union all \
select 'Conceptos Generales' as Dominio, \
'Código de localidades' as Cod_Loc, \
'20180613' as Periodo, \
'RGL_GEN_RN013' as Id_Regla, \
COUNT(1) TotalRegistros, \
sum(case when RGL_GEN_RN013_TG_5 = 1 then '1' else '0' end) as TotalAprobados \
from RSM_T130LOC_FNL \
where RGL_GEN_RN013_TG_5 is not null \
union all \
select 'Conceptos Generales' as Dominio, \
'Código de localidades' as Cod_Loc, \
'20180613' as Periodo, \
'RGL_BTG_RN002' as Id_Regla, \
COUNT(1) TotalRegistros, \
sum(case when RGL_BTG_RN002 = 1 then '1' else '0' end) as TotalAprobados \
from RSM_T130LOC_FNL \
where RGL_BTG_RN002 is not null \
")

DF_RSM_T130LOC.write.option("ignoreLeadingWhiteSpace", "false").option("ignoreTrailingWhiteSpace", "false").csv('/data1/datalake/transformado/tablasgenerales/RSM_T103LOC_TSF',header='true')






# LUEGO EXPORTAR DF_RESUMEN HACIA CSV OJALA DIRECTO SIN PASAR POR HDFS

# EJEMPLO PARA REALIZAR REGLAS DE INTEGRIDAD USANDO SPARK SQL

#DF_T103B21_PCS.createOrReplaceTempView("tabla1")
#DF_T103B21_PCS.createOrReplaceTempView("tabla2")

# cruce right join para mantener los registros de la interface original y marcar con 0 los que no cruzan
# reemplazar RGL_GEN_RN003_TG_16 por el campo de la tabla externa que se valida en la otra tabla en la regla de integridad
#DF_SALIDA_CRUCE = spark.sql(" \
#select tabla1.*, \
#(case when tabla2.RGL_GEN_RN003_TG_16 is null then '0' else '1' end) as rn_integridad \
#from tabla1,tabla2 \
#where tabla1.RGL_GEN_RN003_TG_16=tabla2.RGL_GEN_RN003_TG_16")




