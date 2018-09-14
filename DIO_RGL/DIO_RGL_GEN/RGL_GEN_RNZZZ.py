#DEFINICION DE LA REGLA XXX QUE APLICA FILTRO A LA COLUMNA RESULTADO DE LA APLICAION DE LA REGLA
def RGL_GEN_RNZZZ(REST_RGL,CRUZE):
    if CRUZE == "1":
        if REST_RGL == "0":
            return 0
        elif REST_RGL == "1":
            return 1
    else:
        return ""