# encoding=utf-8
# EJEMPLO DE QUERY RESUMEN PARA LA T103B21 SOLO CON UNA REGLA APLICADA
# REEMPLAZAR T103B21 CON CUALQUIER OTRA INTERFACE
# AGREGAR ESTE CODIGO AL FINAL DE CADA SCRIPT DE EJECUCION DE INTERFACE

# TOMAMOS EL DF CON TODAS LAS REGLAS YA APLICADAS
DF_T103B21_PCS = DF_T103B21_PCS_TMP

# GENERAMOS UNA VISTA TEMPORAL PARA EJECUTAR LAS QUERYS SOBRE LA TABLA RESUMEN
DF_T103B21_PCS.createOrReplaceTempView("resumen")

# CREAMOS UNA TABLA TEMPORAL CON SOLO LOS CAMPOS DE REGLAS

# agregar todas las columnas de reglas dentro del select, una tras otra, ojalá en el mismo orden de la planilla
DF_RESUMEN = spark.sql("select `RGL_GEN_RN003_TG_16` from resumen")

# AHORA EL DF_RESUMEN LO PASAMOS A UNA VISTA TEMPORAL PARA EJECUTAR LAS QUERYS EN ELLA
DF_RESUMEN.createOrReplaceTempView("resumen_final")

# CADA SELECT CORRESPONDE A UNA REGLA, CAMBIAR COD_EDC, ID_REGLA, CASE Y WHERE POR CADA REGLA

#Nota MC: Para que me funcionara tuve que reemplazar las comillas por triples " -> """
DF_RESUMEN = spark.sql(" \
select 'Conceptos Generales' as Dominio, \
'Cod. Actividad Economica SBIF' as Cod_Edc, \
'20181612' as Periodo, \
'RGL_GEN_RN003' as Id_Regla, \
COUNT(1) TotalRegistros, \
sum(case when RGL_GEN_RN003_TG_16=1 then '1' else '0' end) as TotalAprobados \
from resumen_final \
where RGL_GEN_RN003_TG_16 is not null \
union all \
select 'Conceptos Generales' as Dominio, \
'Cod. Actividad Economica SBIF' as Cod_Edc, \
'20181612' as Periodo, \
'RGL_GEN_RN003' as Id_Regla, \
COUNT(1) TotalRegistros, \
sum(case when RGL_GEN_RN003_TG_16=1 then '1' else '0' end) as TotalAprobados \
from resumen_final \
where RGL_GEN_RN003_TG_16 is not null \
")

# LUEGO EXPORTAR DF_RESUMEN HACIA CSV OJALA DIRECTO SIN PASAR POR HDFS

# EJEMPLO PARA REALIZAR REGLAS DE INTEGRIDAD USANDO SPARK SQL

DF_T103B21_PCS.createOrReplaceTempView("tabla1")
DF_T103B21_PCS.createOrReplaceTempView("tabla2")

# cruce right join para mantener los registros de la interface original y marcar con 0 los que no cruzan
# reemplazar RGL_GEN_RN003_TG_16 por el campo de la tabla externa que se valida en la otra tabla en la regla de integridad
DF_SALIDA_CRUCE = spark.sql(" \
select tabla1.*, \
(case when tabla2.`AEC-SBIF-T10` !='' then '0' else '1' end) as RGL_BTG_RN006 \
from tabla1,tabla2 \
where tabla1.`COD-GEN-COD-VAL` = tabla2.`AEC-SBIF-T10`")


DF_SALIDA_CRUCE = spark.sql(" \
select tabla1.*, \
(case when tabla2.`AEC-SBIF-T10` is not null then '1' else '0' end) as RGL_BTG_RN006 \
from tabla1 \
left join tabla2 \
on tabla1.`COD-GEN-COD-VAL` = tabla2.`AEC-SBIF-T10` \
where trim(`COD-GEN-TBL-COD`) = 'ACT-ECO-SBF'")

DF_SALIDA_CRUCE = spark.sql(" \
select tabla1.* \
from tabla1 \
where trim(`COD-GEN-TBL-COD`) = 'ACT-ECO-SBF'")