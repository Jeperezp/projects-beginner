import calendar
import holidays_co
from datetime import date
from datetime import timedelta
import datetime

ahnos = [2021,2022,2023,2024,2025]
lista_f = []
for i in ahnos:
    variable = holidays_co.get_colombia_holidays_by_year(i)
    lista_f.append(variable)

lista_feriados=[]
for i in range(len(lista_f)):
    for j in range(len(lista_f[i])):
        lista_feriados.append(lista_f[i][j][0])

def dias_hab(Fecha,dias):
    lista_dias = []
    lista_Fds_Feriados = []
    contador = 0
    while len(lista_dias)<=dias:
        if (Fecha + timedelta(days=contador)).weekday()==5 or (Fecha + timedelta(days=contador)).weekday()==6:
            lista_Fds_Feriados.append(Fecha + timedelta(days=contador))
        elif Fecha + timedelta(days=contador) in lista_feriados:
            lista_Fds_Feriados.append(Fecha + timedelta(days=contador))
        else:
            lista_dias.append(Fecha + timedelta(days=contador))
        contador = contador + 1
    return max(lista_dias)

