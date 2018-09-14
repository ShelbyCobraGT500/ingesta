import os
"""########## LIMPIEZA DE ARCHIVOS EN HADOOP ##########"""
os.system('hadoop fs -rm -R /data1/datalake/crudo/basecentralclientes/*')
os.system('hadoop fs -rm -R /data1/datalake/crudo/creditomonedanacional/*')
os.system('hadoop fs -rm -R /data1/datalake/crudo/recursoshumanos/*')
os.system('hadoop fs -rm -R /data1/datalake/crudo/tablasgenerales/*')
os.system('hadoop fs -rm -R /data1/datalake/crudo/tarjetadecredito/*')
os.system('hadoop fs -rm -R /data1/datalake/crudo/perfilamiento/*')
"""########## INGESTA DE ARCHIVOS ##########"""
os.system('hadoop fs -put /data1/landing/basecentralclientes/* /data1/datalake/crudo/basecentralclientes/')
os.system('hadoop fs -put /data1/landing/creditomonedanacional/* /data1/datalake/crudo/creditomonedanacional/')
os.system('hadoop fs -put /data1/landing/recursoshumanos/* /data1/datalake/crudo/recursoshumanos/')
os.system('hadoop fs -put /data1/landing/tablasgenerales/* /data1/datalake/crudo/tablasgenerales/')
os.system('hadoop fs -put /data1/landing/tarjetadecredito/* /data1/datalake/crudo/tarjetadecredito/')
os.system('hadoop fs -put /data1/landing/perfilamiento/* /data1/datalake/crudo/perfilamiento/')
"""########## ASIGNACION DE PERMISOS ##########"""
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/basecentralclientes/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/creditomonedanacional/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/recursoshumanos/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/tablasgenerales/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/tarjetadecredito/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/perfilamiento/*')