#!/bin/bash

# CAPTURAMOS CARPETA ACTUAL
dir=$(pwd)

# ELIMINA ARCHIVOS TEMPORALES
rm -f $dir/tmp1
rm -f $dir/tmp2
rm -f $dir/tmp3
rm -f $dir/tmp4
rm -f $dir/tmp5

for f in /data1/desarrollo/edge/landing/tablasgenerales/*;
do a=`echo $f | awk -F/ '{print $NF}'`;
atmp=`echo $f | awk -F/ '{print $NF}' | sed 's/\.[Vv][0-9]*//g'`;
b=`cat $f | wc -l` ;
c=`hadoop fs -cat /data1/desarrollo/hadoop/crudo/tablasgenerales/$a | wc -l`;
d=`cat /data1/desarrollo/edge/postransformado/$atmp.csv | wc -l`;
e=`cat /data1/desarrollo/edge/control/metadato/MET_$atmp.csv | cut -d';' -f5`;
f=`cat /data1/desarrollo/edge/control/metadato/MET_$atmp.csv | cut -d';' -f6`;
echo -e "$a;$b;$c;$d;$e;$f"; done > $dir/tmp1

for f in /data1/desarrollo/edge/landing/recursoshumanos/*;
do a=`echo $f | awk -F/ '{print $NF}'`;
atmp=`echo $f | awk -F/ '{print $NF}' | sed 's/\_[Vv][0-9]*//g'`;
b=`cat $f | wc -l` ;
c=`hadoop fs -cat /data1/desarrollo/hadoop/crudo/recursoshumanos/$a | wc -l`;
d=`cat /data1/desarrollo/edge/postransformado/$atmp.csv | wc -l`;
e=`cat /data1/desarrollo/edge/control/metadato/MET_$atmp.csv | cut -d';' -f5`;
f=`cat /data1/desarrollo/edge/control/metadato/MET_$atmp.csv | cut -d';' -f6`;
echo -e "$a;$b;$c;$d;$e;$f"; done >> $dir/tmp1

for f in /data1/desarrollo/edge/landing/basecentralclientes/*;
do a=`echo $f | awk -F/ '{print $NF}'`;
atmp=`echo $f | awk -F/ '{print $NF}' | sed 's/\.[Vv][0-9]*//g'`;
b=`cat $f | wc -l` ;
c=`hadoop fs -cat /data1/desarrollo/hadoop/crudo/basecentralclientes/$a | wc -l`;
d=`cat /data1/desarrollo/edge/postransformado/$atmp.csv | wc -l`;
e=`cat /data1/desarrollo/edge/control/metadato/MET_$atmp.csv | cut -d';' -f5`;
f=`cat /data1/desarrollo/edge/control/metadato/MET_$atmp.csv | cut -d';' -f6`;
echo -e "$a;$b;$c;$d;$e;$f"; done >> $dir/tmp1

for f in /data1/desarrollo/edge/landing/creditomonedanacional/*;
do a=`echo $f | sed -e s/\.TXT//g | awk -F/ '{print $NF}'`;
atmp=`echo $f | awk -F/ '{print $NF}' | sed 's/\.[Vv][0-9]*//g'`;
b=`cat $f | wc -l` ;
c=`hadoop fs -cat /data1/desarrollo/hadoop/crudo/creditomonedanacional/$a.TXT | wc -l`;
d=`cat /data1/desarrollo/edge/postransformado/$a.csv | wc -l`;
e=`cat /data1/desarrollo/edge/control/metadato/MET_$a.csv | cut -d';' -f5`;
f=`cat /data1/desarrollo/edge/control/metadato/MET_$a.csv | cut -d';' -f6`;
echo -e "$a;$b;$c;$d;$e;$f"; done >> $dir/tmp1

for f in /data1/desarrollo/edge/landing/perfilamiento/*;
do a=`echo $f | awk -F/ '{print $NF}' | sed -e s/\.txt//g`;
atmp=`echo $f | awk -F/ '{print $NF}' | sed 's/\.[Vv][0-9]*//g'`;
b=`cat $f | wc -l`;
c=`hadoop fs -cat /data1/desarrollo/hadoop/crudo/perfilamiento/$a.txt | wc -l`;
d=`cat /data1/desarrollo/edge/postransformado/archivo_parcial_tasi_put_tbj_al_24_07_2018.csv | wc -l`;
e=`cat /data1/desarrollo/edge/control/metadato/MET_archivo_parcial_tasi_put_tbj_al_24_07_2018.csv| cut -d';' -f5`;
f=`cat /data1/desarrollo/edge/control/metadato/MET_archivo_parcial_tasi_put_tbj_al_24_07_2018.csv | cut -d';' -f6`;
echo -e "$a;$b;$c;$d;$e;$f"; done >> $dir/tmp1

# COLUMNAS CALCULADAS DE CONTROL

awk -F ';' '{
if ($3-$5 == 0)
	print $0,";","OK";
else
	print $0,";","ERROR";
}' $dir/tmp1 > $dir/tmp2

awk -F ';' '{
if ($5-$6 == 0)
	print $0,";","SI";
else
	print $0,";","NO";
}' $dir/tmp2 > $dir/tmp3

# HACEMOS JOIN ENTRE ARCHIVOS POR COLUMNA 1 Y 5 RESPECTIVAMENTE
# ELIMINANDO LA CABECERA DEL PARAMETROS.CSV, PARA DESPUES GENERAR CABECERA FINAL

join -1 1 -2 5 -t";" -o 2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,1.2,1.3,1.4,1.5,1.6,1.7,1.8 <(sort -t";" -k 1,1 $dir/tmp3) <(cat /data1/desarrollo/edge/control/Parametros.csv | sed -e '1d' | sed -e 's/\r//g'| sed -e 's/\n//g' | grep -e ";SI;" | sort -t";" -k 5,5) > $dir/tmp4

echo -e "Nombre;Sistema Operativo Origen;Tegnologia Almacenamiento Origen;Tipo de Interface;Nombre Fisico; Fecha de los Datos;Interface Activa;Largo de Fila;Ruta hadoop;Filas landing;Filas ingestado;Filas transformado;Filas archivo metadatos;Filas aprueban R0;Estado ingestado-metadatos;Aprueba R0" > $dir/tmp5

cat $dir/tmp5 $dir/tmp4 > /data1/desarrollo/edge/control/Control_Interfaces_Activas.csv

# ELIMINA ARCHIVOS TEMPORALES
rm -f $dir/tmp1
rm -f $dir/tmp2
rm -f $dir/tmp3
rm -f $dir/tmp4
rm -f $dir/tmp5
