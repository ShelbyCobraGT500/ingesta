#basecentralclientes
echo "LIMPIEZA DE ARCHIVOS EN HADOOP"
start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T107EDP
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T107EDP : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118DCY
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118DCY : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T119DCO
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T119DCO : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T112ROP
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T112ROP : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118DEM
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118DEM : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T119DIR
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T119DIR : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118CLI
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118CLI : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118REN
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118REN : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T718CEP
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T718CEP : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118DCL
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118DCL : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T118SMA
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118SMA : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/basecentralclientes/PBCC.NN.CLIENTES.T718VNT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T718VNT : " $runtime "segundos."

# fin basecentralclientes

#creditomonedanacional
start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CAE_F201803.TXT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CAE_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CCU_F201803.TXT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CCU_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/OPC_F201803.TXT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución OPC_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CCA_F201803.TXT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CCA_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/CIG_F201803.TXT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CIG_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/creditomonedanacional/VEN_F201803.TXT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución VEN_F201803 : " $runtime "segundos."
#creditomonedanacional

#recursoshumanos
start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_BE_TCH_PLANTA
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_BE_TCH_PLANTA : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_REAL_TM_PRD
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_REAL_TM_PRD : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_LU_HR_TYPE
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_LU_HR_TYPE : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4CCL_MOTIVOS_PER_V3
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4CCL_MOTIVOS_PER : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_X_REA_CHANGE
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_X_REA_CHANGE : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_PERSON
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_PERSON : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4RCH_ORGANIZATION
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4RCH_ORGANIZATION : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_ADDRESS
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_ADDRESS : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_PHONE_FAX
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_PHONE_FAX : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SAR_X_CONTRATO
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SAR_X_CONTRATO : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_EMAIL
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_EMAIL : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_WORK_LOCATION
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_WORK_LOCATION : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCL_LIC_SUBSID
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCL_LIC_SUBSID : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_HR_ACAD_BACKGR
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_HR_ACAD_BACKGR : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_WORK_UNIT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_WORK_UNIT : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_H_HR_JOB
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_H_HR_JOB : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_HR_PERIOD
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_HR_PERIOD : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_M4SCO_HR_ROLE
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_HR_ROLE : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/recursoshumanos/PPPL_NN_STD_JOB
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_JOB : " $runtime "segundos."

#fin recursoshumanos

#tablas generales
start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103B21
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103B21 : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103PVD
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103PVD : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703NNS
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703NNS : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103COM
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103COM : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103SEC
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103SEC : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703PUT
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703PUT : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103EJC
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103EJC : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103UNI
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103UNI : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703SGM
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703SGM : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103FRD
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103FRD : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103USR
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103USR : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703SSG
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703SSG : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103LOC
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103LOC : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703AEC
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703AEC : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T103OFI
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103OFI : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tablasgenerales/PBCC.NN.GENERAL.T703MCS
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703MCS : " $runtime "segundos."

#fin tablas generales

#tarjetadecredito

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343BIN.txt
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343BIN : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343BLQ.txt
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343BLQ : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343CTA.txt
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343CTA : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343TAR.txt
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343TAR : " $runtime "segundos."

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/tarjetadecredito/T343TRN_SIN201804.txt
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343TRN_SIN201804 : " $runtime "segundos."

# fin tarjetadecredito

# Perfilamiento

start=`date +%s`
hadoop fs -rm /data1/datalake/crudo/perfilamiento/archivo_parcial_tasi_put_tbj_al_24_07_2018.txt
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución archivo_parcial_tasi_put_tbj_al_24_07_2018 : " $runtime "segundos."

# Fin Perfilamiento

echo "INGESTA DE ARCHIVOS HDFS"

#base central de clientes
start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T107EDP /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T107EDP : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118DCY /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118DCY : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T119DCO /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T119DCO : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T112ROP /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T112ROP : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118DEM /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118DEM : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T119DIR /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T119DIR : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118CLI /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118CLI : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118REN /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118REN : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T718CEP /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T718CEP : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118DCL /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118DCL : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T118SMA /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T118SMA : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/basecentralclientes/PBCC.NN.CLIENTES.T718VNT /data1/datalake/crudo/basecentralclientes/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T718VNT : " $runtime "segundos."

#fin base central de clientes

#inicio credito moneda nacional
start=`date +%s`
hadoop fs -put /data1/landing/creditomonedanacional/CAE_F201803.TXT /data1/datalake/crudo/creditomonedanacional/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CAE_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/creditomonedanacional/CCU_F201803.TXT /data1/datalake/crudo/creditomonedanacional/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CCU_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/creditomonedanacional/OPC_F201803.TXT /data1/datalake/crudo/creditomonedanacional/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución OPC_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/creditomonedanacional/CCA_F201803.TXT /data1/datalake/crudo/creditomonedanacional/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CCA_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/creditomonedanacional/CIG_F201803.TXT /data1/datalake/crudo/creditomonedanacional/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución CIG_F201803 : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/creditomonedanacional/VEN_F201803.TXT /data1/datalake/crudo/creditomonedanacional/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución VEN_F201803 : " $runtime "segundos."

#Fin credito moneda nacional

#Inicio recursos humanos
start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_BE_TCH_PLANTA_V3 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_BE_TCH_PLANTA : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_REAL_TM_PRD_V3 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_REAL_TM_PRD : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_LU_HR_TYPE /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_LU_HR_TYPE : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4CCL_MOTIVOS_PER_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4CCL_MOTIVOS_PER : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_X_REA_CHANGE_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_X_REA_CHANGE : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_PERSON_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_PERSON : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4RCH_ORGANIZATION_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4RCH_ORGANIZATION : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_ADDRESS_V3 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_ADDRESS : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_PHONE_FAX /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_PHONE_FAX : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SAR_X_CONTRATO_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SAR_X_CONTRATO : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_EMAIL_V3 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_EMAIL : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_WORK_LOCATION_v2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_WORK_LOCATION : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCL_LIC_SUBSID_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCL_LIC_SUBSID : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_HR_ACAD_BACKGR_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_HR_ACAD_BACKGR : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_WORK_UNIT /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_WORK_UNIT : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_H_HR_JOB /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_H_HR_JOB : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_HR_PERIOD_V3 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_HR_PERIOD : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_HR_ROLE_v2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_M4SCO_HR_ROLE : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_JOB_V2 /data1/datalake/crudo/recursoshumanos/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución PPPL_NN_STD_JOB : " $runtime "segundos."

#Fin recursos humanos

#Inicio tablas generales
start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103B21 /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103B21 : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103PVD /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103PVD : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703NNS /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703NNS : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103COM /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103COM : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103SEC /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103SEC : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703PUT /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703PUT : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103EJC /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103EJC : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103UNI /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103UNI : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703SGM /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703SGM : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103FRD /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103FRD : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103USR /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103USR : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703SSG /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703SSG : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103LOC /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103LOC : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703AEC /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703AEC : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T103OFI /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T103OFI : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tablasgenerales/PBCC.NN.GENERAL.T703MCS /data1/datalake/crudo/tablasgenerales/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T703MCS : " $runtime "segundos."

#Fin tablas generales

#Inicio tarjeta de credito
start=`date +%s`
hadoop fs -put /data1/landing/tarjetadecredito/T343BIN.txt /data1/datalake/crudo/tarjetadecredito/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343BIN : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tarjetadecredito/T343BLQ.txt /data1/datalake/crudo/tarjetadecredito/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343BLQ : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tarjetadecredito/T343CTA.txt /data1/datalake/crudo/tarjetadecredito/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343CTA : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tarjetadecredito/T343TAR.txt /data1/datalake/crudo/tarjetadecredito/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343TAR : " $runtime "segundos."

start=`date +%s`
hadoop fs -put /data1/landing/tarjetadecredito/T343TRN_SIN201804.txt /data1/datalake/crudo/tarjetadecredito/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343TRN_SIN201804 : " $runtime "segundos."

#Fin tarjeta de credito

#Inicio perfilamiento
start=`date +%s`
hadoop fs -put /data1/landing/perfilamiento/archivo_parcial_tasi_put_tbj_al_24_07_2018.txt /data1/datalake/crudo/perfilamiento/
end=`date +%s`
runtime=$((end-start))
echo "Tiempo ejecución T343TRN_SIN201804 : " $runtime "segundos."
#Fin perfilamiento

########## ASIGNACION DE PERMISOS ##########
echo "Asignacion de permisos CHMOD"
hadoop fs -chmod 777 /data1/datalake/crudo/basecentralclientes/*
hadoop fs -chmod 777 /data1/datalake/crudo/creditomonedanacional/*
hadoop fs -chmod 777 /data1/datalake/crudo/recursoshumanos/*
hadoop fs -chmod 777 /data1/datalake/crudo/tablasgenerales/*
hadoop fs -chmod 777 /data1/datalake/crudo/tarjetadecredito/*
hadoop fs -chmod 777 /data1/datalake/crudo/perfilamiento/*
echo "Fin"

#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_BE_TCH_PLANTA_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCL_LIC_SUBSID_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_X_REA_CHANGE_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_HR_ACAD_BACKGR_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_PERSON_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4RCH_ORGANIZATION_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_HR_ROLE_v2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_ADDRESS_v2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_HR_PERIOD_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_WORK_LOCATION_v2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SAR_X_CONTRATO_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_M4SCO_REAL_TM_PRD_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_EMAIL_V2 /data1/datalake/crudo/recursoshumanos/
#hadoop fs -put /data1/landing/recursoshumanos/PPPL_NN_STD_JOB_V2 /data1/datalake/crudo/recursoshumanos/


