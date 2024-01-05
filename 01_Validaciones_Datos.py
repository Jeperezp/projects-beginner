from datetime import datetime, date, timedelta
import datetime
import requests
import pandas as pd
import teradatasql
import holidays_co
import sys
import logging
import os
import win32com.client as win32

titulo = "Resumen por Fecha"

now = datetime.datetime.now()

if now.day<10:
    dia = "0"+str(now.day)
else:
    dia = str(now.day)

if now.month<10:
    mes = "0"+str(now.month)
else:
    mes = str(now.month)

NombreCarpeta = mes + dia + str(now.year)

#The code is creating a list of years, which includes the current year, the previous year, and the next year.
years = [(now.year)-1,(now.year),(now.year)+1]
days = []
for i in years:
    days.append(holidays_co.get_colombia_holidays_by_year(i))

holydays = []
for i in range(len(days)):
    for j in range(len(days[i])):
        holydays.append(days[i][j][0]) 

if now.weekday()==5 or now.weekday()==6:
    sys.exit()
elif now in holydays:
    sys.exit()
else:
    pass 

#This code is creating a folder and a log file.
ubicacion_fija = os.getcwd()
ruta_carpeta = os.path.join(ubicacion_fija, NombreCarpeta)

if not os.path.exists(ruta_carpeta):
    os.makedirs(ruta_carpeta)
    print(f"Carpeta '{NombreCarpeta}' creada en '{ubicacion_fija}'")
else:
    print(f"La carpeta '{NombreCarpeta}' ya existe en '{ubicacion_fija}'")

NomArchivo_log =       ruta_carpeta+'\\'+ dia + mes + str(now.year) +"F_523_Validacion.log"
logging.basicConfig(filename=NomArchivo_log, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#The code defines a function called `conexion` that takes four parameters: `host`, `user`, `password`, and `query`.
def Connection_TeraData(host, user, password, query):
    try:
        conn = teradatasql.connect(host=host, user=user, password=password)       
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    except Exception as e:       
        logging.warning(f"Error de conexión: {str(e)}")
        return False
    
#The `Connection_DatosAbiertos` function is a Python function that connects to a specified URL and retrieves data from an API.
def Connection_DatosAbiertos(star,end, url):
    all_data = []
    start = f"{star}T00:00:00"
    end = f"{end}T23:59:59"
    params = {
    "$limit": 10000000,
    "$offset": 0,
    "$where": f"fecha_corte >= '{start}' and fecha_corte <='{end}'"
    }
    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Check for any errors in the response
            data = response.json()  # Convert the response to JSON format
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error: {e}")
            data = []

        if not data:  # No more data to fetch, break out of the loop
            break
        all_data.extend(data)
        params["$offset"] += len(data)  # Move the offset for the next request
    df = pd.DataFrame(all_data)
    return df

#The code defines a function called `date_proc` that takes two parameters: `date_sys` and `days_`.
def date_proc(date_sys,days_):
    List_days = []
    list_weekend = []
    contador =1
    while len(List_days)<days_:
        if (date_sys - timedelta(days=contador)).weekday()==5 or (date_sys - timedelta(days=contador)).weekday()==6:
            list_weekend.append(date_sys - timedelta(days=contador))
        elif (date_sys - timedelta(days=contador)) in holydays:
            list_weekend.append(date_sys - timedelta(days=contador))
        else:
            List_days.append(date_sys - timedelta(days=contador))
        contador = contador + 1
    return min(List_days)

#The code you provided defines a function called `dates` that takes in a parameter called `dates`.
def dates (dates):
    date_prod_f = date_proc(dates,2)
    date_prod_i = date_proc(dates,3)

    if (date_prod_f -date_prod_i).days >1:
        star = (date_prod_i +timedelta(days=1))
        end = date_prod_f
    else:
        star = date_prod_f
        end = date_prod_f

    star = date(star.year,star.month,star.day).strftime('%Y-%m-%d')
    end = date(end.year,end.month,end.day).strftime('%Y-%m-%d')
    return star, end

def pares(fecha_min, fecha_max):

    fechas = []
    lista_par = []

    fecha_actual = fecha_min

    while fecha_min <= fecha_max:
        fechas.append(fecha_min.strftime('%Y-%m-%d'))
        
        # Cambiar entre el 1 de enero y el 31 de diciembre
        if fecha_min.month == 1:
            fecha_min = fecha_min.replace(month=12, day=31, year=fecha_min.year)
        else:
            fecha_min = fecha_min.replace(month=1, day=1, year=fecha_min.year + 1)

    # Agregar la fecha de fin si es posterior a la última fecha generada
    if fecha_min > fecha_max:
        fechas.append(fecha_max.strftime('%Y-%m-%d'))

    # Obtener parejas ordenadas
    parejas = [(fechas[i], fechas[i + 1]) for i in range(0, len(fechas) - 1, 2)]

    # Imprimir las parejas
    for pareja in parejas:
        lista_par.append(pareja)
    return lista_par

url = "https://www.datos.gov.co/resource/qhpu-8ixx.json"
hosttd="10.40.176.7"  #hostname o IP de Teradata
userdb="dwh_consulta" #usuario de base de datos
pss = 'dwh_consulta'

Select_from = """SELECT
    dt.FECHA_CORTE
    ,dt.TIPO_ENT Tipo_Entidad
    ,dt.NOM_TENTIDAD Nombre_Tipo_Entidad
    ,dt.COD_ENT Codigo_Entidad
    ,dt.NOM_ENTIDAD Nombre_entidad
    ,dt.TIPO_NEG Tipo_Patrimonio
    ,n.Nombre_Tipo_Patrimonio
    ,dt.SBTIPO_NEG Subtipo_Patrimonio
    ,n.Estado
    ,Case
        When n.Estado = 1 then 'Activo'
        When n.Estado = 2 then 'Liquidación'
        when n.Estado = 3 then 'Inactivo'
    End as Descrip_Estado
    ,n.Cerrado
    ,Case
        When n.Cerrado = 0 then 'Activo'
        When n.Cerrado = 1 then 'Cerrado'
    End as Descrip_Cerrado
    ,n.Fecha_Hasta_Trans
    ,Case
        when dt.FECHA_CORTE <= n.Fecha_Hasta_Trans or  n.Fecha_Hasta_Trans is null then 'Activo'
        else 'Cerrado'
    End as Estado_Real
    ,dt.CODIGO_NEG
    ,dt.PRINC_COMPAR
    ,dt.TIPO_PARTICIPA
    ,case
        when dt.TIPO_PARTICIPA between '100' and '199' then 'Clientes inversionistas'
        when dt.TIPO_PARTICIPA between '200' and '299' then 'Clientes inversionistas'
        when dt.TIPO_PARTICIPA between '300' and '399' then 'Clientes inversionistas'
        when dt.TIPO_PARTICIPA between '400' and '499' then 'Inversionistas institucionales y/o profesionales'
        when dt.TIPO_PARTICIPA between '500' and '599' then 'Clientes inversionistas'
        when dt.TIPO_PARTICIPA between '600' and '699' then 'Cuentas omnibus'
        when dt.TIPO_PARTICIPA between '700' and '799' then 'Otros inversionistas'
        when dt.TIPO_PARTICIPA between '800' and '899' then 'Otros tipos de participaciones'
    Else 'NA'
    End as Descrip_Tip_Pat
    ,dt.REND_ABON
    ,dt.PRECIO_CIERRE
    ,dt.NUM_UNID_D_ANTERIOR
    ,dt.VLR_UNID
    ,dt.APORTE
    ,dt.RETIRO
    ,dt.ANULAC
    ,dt.VLR_FONDO
    ,dt.NUM_INVER
    ,dt.RENT_DIA
    ,dt.RENT_MES
    ,dt.RENT_SEM
    ,dt.RENT_ANUAL
    FROM
    (  SELECT
            t.fecha FECHA_CORTE
            ,t.Anno ANNO
            ,t.Nombre_Mes MES
            ,e.tipo_entidad TIPO_ENT
            ,e.Nombre_Tipo_Entidad NOM_TENTIDAD
            ,e.codigo_entidad COD_ENT
            ,e.Nombre_Entidad NOM_ENTIDAD
            ,i.nivel4 CODRGL
        ,SUM(CASE WHEN i.nivel2 =  1 THEN f.valor END) TIPO_NEG
        ,SUM(CASE WHEN i.nivel2 =  2 THEN f.valor END) SBTIPO_NEG
        ,SUM(CASE WHEN i.nivel2 =  3 THEN f.valor END) CODIGO_NEG
        ,SUM(CASE WHEN i.nivel2 =  4 THEN f.valor END) PRINC_COMPAR
        ,SUM(CASE WHEN i.nivel2 =  5 THEN f.valor END) TIPO_PARTICIPA
        ,SUM(CASE WHEN i.nivel2 =  6 THEN f.valor END) REND_ABON
        ,SUM(CASE WHEN i.nivel2 =  7 THEN f.valor END) PRECIO_CIERRE
        ,SUM(CASE WHEN i.nivel2 =  8 THEN f.valor END) NUM_UNID_D_ANTERIOR
        ,SUM(CASE WHEN i.nivel2 =  9 THEN f.valor END) VLR_UNID
        ,SUM(CASE WHEN i.nivel2 = 10 THEN f.valor END) APORTE
        ,SUM(CASE WHEN i.nivel2 = 11 THEN f.valor END) RETIRO
        ,SUM(CASE WHEN i.nivel2 = 12 THEN f.valor END) ANULAC
        ,SUM(CASE WHEN i.nivel2 = 13 THEN f.valor END) VLR_FONDO
        ,SUM(CASE WHEN i.nivel2 = 14 THEN f.valor END) NUM_INVER
        ,SUM(CASE WHEN i.nivel2 = 15 THEN f.valor END) RENT_DIA
        ,SUM(CASE WHEN i.nivel2 = 16 THEN f.valor END) RENT_MES
        ,SUM(CASE WHEN i.nivel2 = 17 THEN f.valor END) RENT_SEM
        ,SUM(CASE WHEN i.nivel2 = 18 THEN f.valor END) RENT_ANUAL
            FROM
            PROD_DWH_CONSULTA.INSUMO_ENTIDAD f
            JOIN PROD_DWH_CONSULTA.INSUMOS i ON i.inm_id=f.inm_id
            JOIN PROD_DWH_CONSULTA.TIEMPO t ON t.tie_id=f.tie_id
            JOIN PROD_DWH_CONSULTA.ENTIDADES e ON e.ent_id=f.ent_id     
            WHERE i.nivel1=523 AND i.tipo_informe=77
            GROUP BY
            t.fecha
            ,t.Anno
            ,t.Nombre_Mes
            ,e.tipo_entidad
            ,e.Nombre_Tipo_Entidad
            ,e.codigo_entidad
            ,e.Nombre_Entidad
            ,i.nivel4        
            ) dt
        JOIN PROD_DWH_CONSULTA.PATRIMONIOS_AUTONOMOS n
                ON n.Tipo_Entidad = dt.TIPO_ENT
                AND n.Codigo_Entidad = dt.COD_ENT
                AND n.Subtipo_Patrimonio = dt.SBTIPO_NEG
                AND n.Codigo_Patrimonio = dt.CODIGO_NEG      
    WHERE"""

#The code snippet is checking the current day of the week using the `now.weekday()` function.
days_works=[0,1,2,3]

#The code snippet is checking the current day of the week using the `now.weekday()` function.
days_works=[0,1,2,3]

if now.weekday() in days_works:
    start, end = dates(now)
    logging.info(f"Fecha Proceso: {datetime.datetime.now()}")
    logging.info(f"Fecha Corte  : {start}")
    try:
        logging.info(f"Estableciendo conexion Datos Abiertos {url}")
        df_datos_abiertos = Connection_DatosAbiertos(start,end,url)
        logging.info(f"Datos Cargados cortes {start} - {end}")
    except:
        logging.warning(f"No se logro establecer conecxion {url}")
    
    try:
        where_ = f""" dt.FECHA_CORTE between '{start}' and '{end}'
        and codigo_neg <>1
        Order by 1,2,3,4,6"""
        query = f"{Select_from}\n{where_}"
        logging.info(f"Estableciendo conexion TeraData")
        df_TeraData = Connection_TeraData(hosttd, userdb, pss, query)
        logging.info(f"Datos Cargados Teradata cortes {start} - {end}")
    except:
        logging.warning(f"No se logro establecer conecxion")

elif now.weekday()==4:
    dataframes_resultantes = []
    dataframes_resultantes_da = []
    start = datetime.datetime(year=2016,month=1,day=1)
    end  = date_proc(now,2)
    periodos = pares(start,end)
    logging.info(f'El periodo de datos evaluados son: {start} - {end}')
    logging.info(f"Estableciendo conexion TeraData")
    
    for i in range(len(periodos)):
        where_ = f""" dt.FECHA_CORTE between '{periodos[i][0]}' and '{periodos[i][1]}'
        and codigo_neg <>1
        Order by 1,2,3,4,6"""
        query = f"{Select_from}\n{where_}"
        dataframes_resultantes.append(Connection_TeraData(hosttd, userdb, pss, query))
        logging.info(f'se cargaron los datos de Teradata para el periodo de {periodos[i][0]} al {periodos[i][1]}')
        print(f'se cargaron los datos de Teradata para el periodo de {periodos[i][0]} al {periodos[i][1]}')

    logging.info(f"Estableciendo conexion Datos Abiertos {url}")   
    for i in range (len(periodos)):
        dataframes_resultantes_da.append(Connection_DatosAbiertos(periodos[i][0],periodos[i][1],url))
        logging.info(f'se cargaron los datos de datos Abiertos para el periodo de {periodos[i][0]} al {periodos[i][1]}')
        print(f'se cargaron los datos de datos Abiertos para el periodo de {periodos[i][0]} al {periodos[i][1]}')
    
    df_TeraData = pd.concat(dataframes_resultantes,axis=0) 
    logging.info(f'Finalizo el Proceso de Consolidacion TeraData')
    df_datos_abiertos = pd.concat(dataframes_resultantes_da, axis = 0)
    logging.info(f'Finalizo el Proceso de Consolidacion Datos Abiertos')
else:
    sys.exit()

if df_datos_abiertos.shape[0]==0 and now.weekday() in days_works:
    logging.warning(f"No hay datos Cargados para la fecha de corte {start}")
    logging.warning(f"Finaliza Proceso")

    outlook = win32.Dispatch('Outlook.Application')
    namespace = outlook.GetNamespace("MAPI")

    # Crear un objeto de correo electrónico
    mail = outlook.CreateItem(0)

    # Puedes proporcionar una lista de direcciones de correo electrónico separadas por punto y coma (;)
    recipients = ['jcrojas@superfinanciera.gov.co','nlbautista@superfinanciera.gov.co','kspiragua@superfinanciera.gov.co']
    recipients_cc = ['lmvalencia@superfinanciera.gov.co']
    mail.To = ";".join(recipients)
    mail.cc = ";".join(recipients_cc)

    # Configurar el correo electrónico
    mail.Subject = str(mes) + str(dia) + str(now.year)+' Validacion Informacion F523 Datos Abiertos'

    mail.Body = f"""
        Buen dia

        De manera atenta nos permitimos informar que no se registran datos Cargados en Datos Abiertos para el corte {start}

        Gracias
    """

    mail.Attachments.Add(NomArchivo_log)

    # Enviar el correo electrónico
    mail.Send()

    logging.info('correo Enviado Exitosamente')
    logging.shutdown()
    sys.exit()
else:
    logging.info(f'Los Datos Cargados registrados en TeraData son:{df_TeraData.shape[0]}')
    logging.info(f'Los Datos Cargados registrados en Datos Abiertos son:{df_datos_abiertos.shape[0]}')
    logging.info('Nota: Que la cantidad de registros coincida no es Sinonimo que no se presenten diferencias')
    Columns_df = list(df_datos_abiertos.columns)
    Columns_TeraData = list(df_TeraData.columns)

    Columns_df_m = []
    Columns_TeraDta_m = []

    for i in Columns_df:
        Columns_df_m.append(i.lower())

    for i in Columns_TeraData:
        Columns_TeraDta_m.append(i.lower())

    df_datos_abiertos.columns = Columns_df_m
    df_TeraData.columns = Columns_TeraDta_m

    Change_dtypes_TeraData= {
    

    "tipo_entidad":"category",
    "nombre_tipo_entidad":"category",
    "codigo_entidad":"uint8",
    "nombre_entidad":"object",
    "tipo_patrimonio":"uint8",
    "nombre_tipo_patrimonio":"object",
    "subtipo_patrimonio":"uint8",
    "estado":"uint8",
    "descrip_estado":"category",
    "cerrado":"uint8",
    "descrip_cerrado":"category",
    "estado_real":"category",
    "codigo_neg":"uint32",
    "princ_compar":"uint8",
    "tipo_participa":"uint16",
    "descrip_tip_pat":"category"
    }

    Change_dtypes_DatosAbiertos = {
    "tipo_entidad":"category",
    "nombre_tipo_entidad":"category",
    "codigo_entidad":"uint8",
    "nombre_entidad":"object",
    "tipo_negocio":"uint8",
    "nombre_tipo_patrimonio":"object",
    "subtipo_negocio":"uint8",
    "nombre_subtipo_patrimonio":"category",
    "codigo_negocio":"uint32",
    "nombre_patrimonio":"object",
    "principal_compartimento":"uint8",
    "tipo_participacion":"uint16"

    }

    df_TeraData.fecha_corte = pd.to_datetime(df_TeraData.fecha_corte, format='%d/%m/%Y')
    df_datos_abiertos.fecha_corte = pd.to_datetime(df_datos_abiertos.fecha_corte)#, format='%d/%m/%Y')
    #df_TeraData.fecha_hasta_trans = pd.to_datetime(df_TeraData.fecha_hasta_trans,format = '%d/%m/%Y')

    for i in Change_dtypes_DatosAbiertos.keys():
        df_datos_abiertos[i.lower()] = df_datos_abiertos[i].astype(Change_dtypes_DatosAbiertos[i])


    for i in Change_dtypes_TeraData.keys():
        df_TeraData[i] = df_TeraData[i].astype(Change_dtypes_TeraData[i])

base_date = datetime.datetime(1900, 1, 1)

def fecha_a_numero_serie(fecha):
    diferencia = (fecha - base_date).days
    return diferencia+2


df_TeraData["Mes"] = (df_TeraData["fecha_corte"].dt.month).astype("uint8")
df_TeraData["Anho"] = (df_TeraData["fecha_corte"].dt.year).astype("uint16")
df_TeraData["Fecha_num"] = (df_TeraData["fecha_corte"].apply(fecha_a_numero_serie)).astype("uint16")
df_TeraData["fecha_corte"] = df_TeraData["fecha_corte"].dt.strftime('%d/%m/%Y')

df_datos_abiertos["Mes"] = df_datos_abiertos.fecha_corte.dt.month
df_datos_abiertos["Anho"] = df_datos_abiertos.fecha_corte.dt.year
#df_datosAbiertos['NombreMes'] = df_datos_abiertos['fecha_corte'].dt.month.apply(lambda x: calendar.month_name[x])
df_datos_abiertos["Mes"] = df_datos_abiertos["Mes"].astype("uint8")
df_datos_abiertos["Anho"] = df_datos_abiertos["Anho"].astype("uint16")
df_datos_abiertos["Fecha_num"] = (df_datos_abiertos["fecha_corte"].apply(fecha_a_numero_serie)).astype("uint16")
df_datos_abiertos["fecha_corte"] = df_datos_abiertos["fecha_corte"].dt.strftime('%d/%m/%Y')

df_TeraData["Key"] = df_TeraData["Fecha_num"].astype("str")+df_TeraData["codigo_neg"].astype("str")
df_datos_abiertos["Key"] = df_datos_abiertos["Fecha_num"].astype("str")+df_datos_abiertos["codigo_negocio"].astype("str")

GrupoTeraData = df_TeraData.groupby(['Key',"fecha_corte"]).agg({'Key':'count'}).rename(columns={"Key": "cantidad"}).reset_index()
GrupoDatosAbiertos = df_datos_abiertos.groupby(['Key','fecha_corte']).agg({'Key':'count'}).rename(columns={"Key": "cantidad"}).reset_index()

Df_Detalle_Diferencias = pd.merge(
    left=GrupoTeraData,
    right=GrupoDatosAbiertos,
    on = ["Key", "fecha_corte"],
    how='outer',
    suffixes=('_TeraData','_Datos_Abiertos'))

Df_Detalle_Diferencias = Df_Detalle_Diferencias.fillna(0)

def diferencias(row,campo1, campo2):
    if row[campo1] > row[campo2]:
        return row[campo1] - row[campo2]
    else:
        return row[campo2] - row[campo1]

Df_Detalle_Diferencias['Diferencia'] =  Df_Detalle_Diferencias.apply(diferencias,axis=1,args=('cantidad_TeraData','cantidad_Datos_Abiertos'))

def validacion(valor):
    if pd.isna(valor):
        return 'sin Dato'
    elif valor ==0:
        return 'Sin Diferencia'
    elif valor !=0:
        return 'Diferencia'
           

Df_Detalle_Diferencias['Validacion'] = Df_Detalle_Diferencias["Diferencia"].apply(validacion)
NombreArchivo1 = ruta_carpeta+'\\'+ dia + mes + str(now.year) + ' Consolidado_Diferencias_F523.txt'
Df_Detalle_Diferencias['Validacion'].unique()

if Df_Detalle_Diferencias['Diferencia'].sum()==0:
    logging.info(f'No se presentaron diferencias en la validacion por fechas')
else:
    logging.warning(f"Total diferencias encontradas: {Df_Detalle_Diferencias['Diferencia'].sum()}")
    logging.warning(f'El Archivo con el Detalle de las diferencias se encuentra ubicado en: {NombreArchivo1}')
    Df_Detalle_Diferencias[(Df_Detalle_Diferencias['Validacion']=='Diferencia')|(Df_Detalle_Diferencias['Validacion']=='sin Dato')].groupby(['fecha_corte']).agg({"cantidad_TeraData":'sum',"cantidad_Datos_Abiertos":'sum','Diferencia':'sum'}).reset_index().to_csv(NombreArchivo1,sep='|',index=False)

def aplicar_logica(row,campo1,campo2):
    if pd.isna(row[campo1]) and not pd.isna(row[campo2]):
        return f'Validar {campo1}'
    elif not pd.isna(row[campo1]) and pd.isna(row[campo2]):
        return f'Validar {campo2}'
    elif row[campo1] - row[campo2]==0:
        return "sin Diferencia"
    elif row[campo1] > row[campo2]:
        return f'El campo {campo1} tiene mas registros que el {campo2}'
    elif row[campo1] < row[campo2]:
        return f'El campo {campo2} tiene mas registros que el {campo1}'    
    else:
        return "evaluar"

Df_Detalle_Diferencias['observacion'] = Df_Detalle_Diferencias.apply(aplicar_logica,axis=1,args=('cantidad_TeraData','cantidad_Datos_Abiertos'))
Df_Detalle_Diferencias.columns =['Key_','fecha_corte_','cantidad_TeraData_','cantidad_Datos_Abiertos_','Diferencia_','Validacion_','observacion_']
Df_Detalle_Diferencias['observacion_'].unique()


#The code is performing an inner merge operation between the `df_datos_abiertos` DataFrame and a subset of the `Df_Detalle_Diferencias` DataFrame. The subset is filtered based on the condition `Df_Detalle_Diferencias['diferencia_']=='Validar cantidad_TeraData'`. The merge is performed on the columns 'Key' from the right DataFrame and 'Key_' from the left DataFrame. The resulting DataFrame includes only the columns from `df_datos_abiertos`.
df_detalle_No_reg_Tda_1 = pd.merge(
    right=df_datos_abiertos,
    left=Df_Detalle_Diferencias[(Df_Detalle_Diferencias['observacion_']=='Validar cantidad_TeraData')],
    how='inner',
    right_on = ['Key'],
    left_on = ['Key_']
)[df_datos_abiertos.columns]

#The code is performing a series of merge operations to identify and extract specific rows from the `df_datos_abiertos` DataFrame.
df_detalle_No_reg_Tda_2 = pd.merge(
    right=df_datos_abiertos,
    left=Df_Detalle_Diferencias[(Df_Detalle_Diferencias['observacion_']=='El campo cantidad_Datos_Abiertos tiene mas registros que el cantidad_TeraData')],
    how='inner',
    right_on = ['Key'],
    left_on = ['Key_']
)[['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participacion','codigo_negocio']]

df_detalle_No_reg_Tda_2 = pd.merge(
    right = df_detalle_No_reg_Tda_2,
    left = df_TeraData,
    how = 'right',
    right_on = ['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participacion','codigo_negocio'],
    left_on = ['Key','fecha_corte', 'tipo_entidad','codigo_entidad','tipo_participa','codigo_neg'],
    suffixes = ["","_A"]
)[['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participacion','codigo_negocio','nombre_tipo_patrimonio']]

df_detalle_No_reg_Tda_2 = df_detalle_No_reg_Tda_2[df_detalle_No_reg_Tda_2['nombre_tipo_patrimonio'].isna()]

df_detalle_No_reg_Tda_2 = pd.merge(
    right=df_datos_abiertos,
    left=df_detalle_No_reg_Tda_2[['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participacion','codigo_negocio']],
    how='inner',
    right_on = ['Key','tipo_participacion'],
    left_on = ['Key_','tipo_participacion'],
    suffixes = ['','_p']
)[df_datos_abiertos.columns]

df_No_registrados_TeraData = pd.concat([df_detalle_No_reg_Tda_2,df_detalle_No_reg_Tda_1],axis=0)

df_detalle_No_reg_Da_1 = pd.merge(
    right=df_TeraData,
    left=Df_Detalle_Diferencias[(Df_Detalle_Diferencias['observacion_']=='Validar cantidad_Datos_Abiertos')],
    how='inner',
    right_on = ['Key'],
    left_on = ['Key_']
)[df_TeraData.columns]


df_detalle_No_reg_Da_2 = pd.merge(
    right=df_TeraData,
    left=Df_Detalle_Diferencias[(Df_Detalle_Diferencias['observacion_']=='El campo cantidad_TeraData tiene mas registros que el cantidad_Datos_Abiertos')],
    how='inner',
    right_on = ['Key'],
    left_on = ['Key_']
)[['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participa','codigo_neg']]


df_detalle_No_reg_Da_2 = pd.merge(
    right = df_detalle_No_reg_Da_2,
    left = df_datos_abiertos,
    how = 'right',
    right_on = ['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participa','codigo_neg'],
    left_on = ['Key','fecha_corte', 'tipo_entidad','codigo_entidad','tipo_participacion','codigo_negocio'],
    suffixes = ["","_A"]
)[['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participa','codigo_neg','nombre_tipo_entidad']]

df_detalle_No_reg_Da_2 = df_detalle_No_reg_Da_2[df_detalle_No_reg_Da_2['nombre_tipo_entidad'].isna()]


df_detalle_No_reg_Da_2 = pd.merge(
    right=df_TeraData,
    left=df_detalle_No_reg_Da_2[['Key_','fecha_corte','tipo_entidad','codigo_entidad','tipo_participa','codigo_neg']],
    how='inner',
    right_on = ['Key','tipo_participa'],
    left_on = ['Key_','tipo_participa'],
    suffixes = ['','_p']
)[df_TeraData.columns]

df_No_registrados_Datos_Abiertos = pd.concat([df_detalle_No_reg_Da_2,df_detalle_No_reg_Da_1],axis=0)

NombreArchivo2 = ruta_carpeta+'\\'+ dia + mes + str(now.year) + ' NoRegTeraData.txt'
if df_No_registrados_TeraData.shape[0]>0:
    logging.warning(f"Cantidad registros que no se visualizan en TeraData: {df_No_registrados_TeraData.shape[0]}")
    logging.warning(f"El Archivo que contiene el Detalle de los registros se encuentra ubicado en: {NombreArchivo2}")
    df_No_registrados_TeraData.to_csv(NombreArchivo2,sep='|', index=False)
else:
    logging.info('la totalidad de registros en Teradata se reflejan en los datos cargados en Datos Abiertos')

NombreArchivo3 = ruta_carpeta+'\\'+ dia + mes + str(now.year) + ' NoRegDatosAbiertos.txt'
if df_No_registrados_TeraData.shape[0]>0:
    logging.warning(f"Cantidad registros que no se visualizan en Datos Abiertos: {df_No_registrados_Datos_Abiertos.shape[0]}")
    logging.warning(f"El Archivo que contiene el Detalle de los registros se encuentra ubicado en: {NombreArchivo3}")
    df_No_registrados_Datos_Abiertos.to_csv(NombreArchivo3,sep='|', index=False)
else:
    logging.info('la totalidad de registros de datos Abiertos se registran en TeraData')

if now.weekday()==4:
    df_codigo_Neg = df_TeraData[['fecha_corte','codigo_neg','tipo_entidad','codigo_entidad']]
    df_codigo_Neg = df_codigo_Neg.groupby(['codigo_neg','fecha_corte']).agg({'fecha_corte':'count'}).rename(columns={"fecha_corte": "cantidad"}).reset_index()
    df_codigo_Neg["fecha_corte"] = pd.to_datetime(df_codigo_Neg["fecha_corte"],format='%d/%m/%Y')

    Limites = df_codigo_Neg.groupby(['codigo_neg']).agg(fecha_minima=pd.NamedAgg(column='fecha_corte', aggfunc='min'),
                                                        fecha_maxima=pd.NamedAgg(column='fecha_corte', aggfunc='max'),
                                                        Cantidad = pd.NamedAgg(column='fecha_corte',aggfunc='count')).reset_index()

    Limites["R_Teoricos"] = ((Limites.fecha_maxima - Limites.fecha_minima).dt.days)+1
    Limites["Diferencia"] = Limites.R_Teoricos - Limites.Cantidad

    df_privote = pd.pivot_table(df_codigo_Neg,values='codigo_neg',index=["codigo_neg"],columns=["fecha_corte"],aggfunc='count',fill_value='Vacio')
    matriz = df_privote=='Vacio'

    valores_nulos = []
    for columna in matriz.columns:
        for i, indice in matriz.iterrows():
            valor = matriz.loc[i, columna]
            if valor ==True:
                #print(valor, columna, i)
                lista = [valor, columna, i]
                valores_nulos.append(lista)

    nulos = pd.DataFrame(valores_nulos,columns=["validacion","Fecha","codigo_Negocio"])

    Faltantes = pd.merge(left =Limites,
                        right = nulos,
                        how = "inner",
                        left_on = ['codigo_neg'],
                        right_on = ['codigo_Negocio'],
                        suffixes = ["_t",'_l'])


    Faltantes = Faltantes[(Faltantes['Fecha']>=Faltantes['fecha_minima'])&(Faltantes['Fecha']<=Faltantes['fecha_maxima'])]
    Faltantes["Fecha_proceso"] = datetime.datetime.now()
    NombreArchivo4 = ruta_carpeta+'\\'+ dia + mes + str(now.year) + ' Registros_faltantes.txt'
    Faltantes.to_csv(NombreArchivo4,sep='|',index=False)
else:
    pass

Mensaje1 = f"""
    Buen dia

    de Manera atenta nos permitimos informar que el proceso de validacion de datos entre Datos Abiertos y Teradata ha finalizando evidenciando las siguientes difencias
     - Total diferencias entre los archivos {Df_Detalle_Diferencias['Diferencia_'].sum()}
     - Registros que se encuentran en Datos Abiertos pero NO en Teradata:{df_No_registrados_TeraData.shape[0]}
     - Registros que se encuentran en Teradata y NO en Datos Abiertos:{df_No_registrados_Datos_Abiertos.shape[0]}

    Se anexan los archivos correspondientes

    quedamos atentos a sus comentarios
"""

Mensaje2 = f"""
    Buen dia

    de Manera atenta nos permitimos informar que el proceso de validacion entre Datos Abiertos y Teradata fue exitoso y no se detectaron diferencias, se anexa el Log con el registro del proceso
    
    Muchas gracias y quedamos atentos a sus comentarios 
"""

# Crear una instancia de Outlook
outlook = win32.Dispatch('Outlook.Application')
namespace = outlook.GetNamespace("MAPI")

# Crear un objeto de correo electrónico
mail = outlook.CreateItem(0)

# Puedes proporcionar una lista de direcciones de correo electrónico separadas por punto y coma (;)
recipients = ['jcrojas@superfinanciera.gov.co','nlbautista@superfinanciera.gov.co','kspiragua@superfinanciera.gov.co']
recipients_cc = ['lmvalencia@superfinanciera.gov.co']
mail.To = ";".join(recipients)
mail.cc = ";".join(recipients_cc)

# Configurar el correo electrónico
mail.Subject = str(mes) + str(dia) + str(now.year)+' Validacion Informacion F523 Datos Abiertos'

if Df_Detalle_Diferencias['Diferencia_'].sum()>0:
    mail.Body = Mensaje1
    mail.Attachments.Add(NomArchivo_log)
    mail.Attachments.Add(NombreArchivo1)
    mail.Attachments.Add(NombreArchivo2)
    mail.Attachments.Add(NombreArchivo3)
else:
    mail.Body = Mensaje2
    mail.Attachments.Add(NomArchivo_log)

# Enviar el correo electrónico
mail.Send()

logging.info('correo Enviado Exitosamente')
logging.shutdown()


del GrupoDatosAbiertos
del GrupoTeraData
del Limites
del df_No_registrados_Datos_Abiertos
del df_No_registrados_TeraData
del df_TeraData
del df_codigo_Neg
del df_detalle_No_reg_Da_1
del df_detalle_No_reg_Da_2
del df_detalle_No_reg_Tda_1
del df_detalle_No_reg_Tda_2
del df_privote
del matriz
del nulos
del Df_Detalle_Diferencias
del Faltantes
del df_datos_abiertos
