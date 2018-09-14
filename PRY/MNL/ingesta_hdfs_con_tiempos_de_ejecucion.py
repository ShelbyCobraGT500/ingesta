import os
import time

########## LIMPIEZA DE ARCHIVOS EN HADOOP ##########
print('Borrado de archivos en hadoop')
start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T107EDP')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T107EDP fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118DCY')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118DCY fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T119DCO')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T119DCO fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T112ROP')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T112ROP fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118DEM')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118DEM fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T119DIR')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T119DIR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118CLI')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118CLI fue:', exec_time/60, 'Minutos'

#start_time = time.time()
#os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118REN')
#end_time = time.time()
#exec_time = end_time-start_time 
#print 'El tiempo de ejecucion para T118REN fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T718CEP')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T718CEP fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118DCL')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118DCL fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118SMA')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118SMA fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T718VNT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T718VNT fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CAE_F201803.TXT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CAE_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CCU_F201803.TXT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CCU_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/OPC_F201803.TXT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para OPC_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CCA_F201803.TXT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CCA_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CIG_F201803.TXT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CIG_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/VEN_F201803.TXT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para VEN_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_BE_TCH_PLANTA')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_BE_TCH_PLANTA fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_REAL_TM_PRD')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_REAL_TM_PRD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_LU_HR_TYPE')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_LU_HR_TYPE fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4CCL_MOTIVOS_PER')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4CCL_MOTIVOS_PER fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_X_REA_CHANGE')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_X_REA_CHANGE fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_PERSON')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_PERSON fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4RCH_ORGANIZATION')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4RCH_ORGANIZATION fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_ADDRESS')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_ADDRESS fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_PHONE_FAX')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_PHONE_FAX fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SAR_X_CONTRATO')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SAR_X_CONTRATO fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_EMAIL')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_EMAIL fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_WORK_LOCATION')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_WORK_LOCATION fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCL_LIC_SUBSID')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCL_LIC_SUBSID fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_HR_ACAD_BACKGR')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_HR_ACAD_BACKGR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_WORK_UNIT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_WORK_UNIT fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_H_HR_JOB')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_H_HR_JOB fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_HR_PERIOD')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_HR_PERIOD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_HR_ROLE')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_HR_ROLE fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_JOB')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_JOB fue:', exec_time/60, 'Minutos'

#start_time = time.time()
#os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103B21')
#end_time = time.time()
#exec_time = end_time-start_time 
#print 'El tiempo de ejecucion para T103B21 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103PVD')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103PVD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703NNS')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703NNS fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103COM')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103COM fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103SEC')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103SEC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703PUT')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703PUT fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103EJC')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103EJC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103UNI')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103UNI fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703SGM')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703SGM fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103FRD')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103FRD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103USR')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103USR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703SSG')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703SSG fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103LOC')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103LOC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703AEC')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703AEC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103OFI')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103OFI fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703MCS')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703MCS fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343BIN.txt')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343BIN fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343BLQ.txt')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343BLQ fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343CTA.txt')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343CTA fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343TAR.txt')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343TAR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343TRN_SIN201804.txt')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343TRN_SIN201804 fue:', exec_time/60, 'Minutos'

#start_time = time.time()
#os.system('hadoop fs -rm /data1/datalake/crudo/perfilamiento/archivo_parcial_tasi_put_tbj_al_24_07_2018.txt')
#end_time = time.time()
#exec_time = end_time-start_time 
#print 'El tiempo de ejecucion para archivo_parcial_tasi_put_tbj_al_24_07_2018 fue:', exec_time/60, 'Minutos'

########## INGESTA DE ARCHIVOS ##########
print('Ingesta de archivos')
start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T107EDP /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T107EDP fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118DCY /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118DCY fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T119DCO /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T119DCO fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T112ROP /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T112ROP fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118DEM /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118DEM fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T119DIR /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T119DIR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118CLI /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118CLI fue:', exec_time/60, 'Minutos'

#start_time = time.time()
#os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118REN /data1/datalake/crudo/basecentralclientes/')
#end_time = time.time()
#exec_time = end_time-start_time 
#print 'El tiempo de ejecucion para T118REN fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T718CEP /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T718CEP fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118DCL /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118DCL fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118SMA /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T118SMA fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T718VNT /data1/datalake/crudo/basecentralclientes/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T718VNT fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/creditomonedanacional/CAE_F201803.TXT /data1/datalake/crudo/creditomonedanacional/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CAE_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/creditomonedanacional/CCU_F201803.TXT /data1/datalake/crudo/creditomonedanacional/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CCU_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/creditomonedanacional/OPC_F201803.TXT /data1/datalake/crudo/creditomonedanacional/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para OPC_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/creditomonedanacional/CCA_F201803.TXT /data1/datalake/crudo/creditomonedanacional/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CCA_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/creditomonedanacional/CIG_F201803.TXT /data1/datalake/crudo/creditomonedanacional/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para CIG_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/creditomonedanacional/VEN_F201803.TXT /data1/datalake/crudo/creditomonedanacional/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para VEN_F201803 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_BE_TCH_PLANTA /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_BE_TCH_PLANTA fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_REAL_TM_PRD /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_REAL_TM_PRD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_LU_HR_TYPE /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_LU_HR_TYPE fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4CCL_MOTIVOS_PER /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4CCL_MOTIVOS_PER fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_X_REA_CHANGE /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_X_REA_CHANGE fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_PERSON /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_PERSON fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4RCH_ORGANIZATION /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4RCH_ORGANIZATION fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_ADDRESS /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_ADDRESS fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_PHONE_FAX /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_PHONE_FAX fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SAR_X_CONTRATO /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SAR_X_CONTRATO fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_EMAIL /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_EMAIL fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_WORK_LOCATION /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_WORK_LOCATION fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCL_LIC_SUBSID /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCL_LIC_SUBSID fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_HR_ACAD_BACKGR /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_HR_ACAD_BACKGR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_WORK_UNIT /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_WORK_UNIT fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_H_HR_JOB /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_H_HR_JOB fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_HR_PERIOD /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_HR_PERIOD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_HR_ROLE /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_M4SCO_HR_ROLE fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_JOB /data1/datalake/crudo/recursoshumanos/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para PPPL_NN_STD_JOB fue:', exec_time/60, 'Minutos'

#start_time = time.time()
#os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103B21 /data1/datalake/crudo/tablasgenerales/')
#end_time = time.time()
#exec_time = end_time-start_time 
#print 'El tiempo de ejecucion para T103B21 fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103PVD /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103PVD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703NNS /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703NNS fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103COM /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103COM fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103SEC /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103SEC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703PUT /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703PUT fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103EJC /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103EJC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103UNI /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103UNI fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703SGM /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703SGM fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103FRD /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103FRD fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103USR /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103USR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703SSG /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703SSG fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103LOC /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103LOC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703AEC /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703AEC fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103OFI /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T103OFI fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703MCS /data1/datalake/crudo/tablasgenerales/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T703MCS fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tarjetadecredito/T343BIN.txt /data1/datalake/crudo/tarjetadecredito/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343BIN fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tarjetadecredito/T343BLQ.txt /data1/datalake/crudo/tarjetadecredito/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343BLQ fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tarjetadecredito/T343CTA.txt /data1/datalake/crudo/tarjetadecredito/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343CTA fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tarjetadecredito/T343TAR.txt /data1/datalake/crudo/tarjetadecredito/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343TAR fue:', exec_time/60, 'Minutos'

start_time = time.time()
os.system('hadoop fs -put /data1/landing/tarjetadecredito/T343TRN_SIN201804.txt /data1/datalake/crudo/tarjetadecredito/')
end_time = time.time()
exec_time = end_time-start_time 
print 'El tiempo de ejecucion para T343TRN_SIN201804 fue:', exec_time/60, 'Minutos'

#start_time = time.time()
#os.system('hadoop fs -put /data1/landing/perfilamiento/archivo_parcial_tasi_put_tbj_al_24_07_2018.txt /data1/datalake/crudo/perfilamiento/')
#end_time = time.time()
#exec_time = end_time-start_time 
#print 'El tiempo de ejecucion para archivo_parcial_tasi_put_tbj_al_24_07_2018 fue:', exec_time/60, 'Minutos'

print 'Asignacion de permisos'
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/basecentralclientes/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/creditomonedanacional/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/recursoshumanos/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/tablasgenerales/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/tarjetadecredito/*')
os.system('hadoop fs -chmod 777 /data1/datalake/crudo/perfilamiento/*')