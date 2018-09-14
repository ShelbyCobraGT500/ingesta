from datetime import datetime
def ORN_32_001(input1, input2):
#ESTABLECE EL FORMATO DE FECHA QUE VALIDARÁ
  formato_fecha = '%Y%m%d'
  try:
      if datetime.strptime(input1,formato_fecha) and datetime.strptime(input2,formato_fecha):
          #SI EL FORMATO CORRESPONDE AL INDICADO, RETORNA 1
          return 1
      else:
          return 0
  except ValueError:
      return 0