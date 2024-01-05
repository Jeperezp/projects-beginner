import  datetime
from  ClassConnection import Conexion_TDTA 
import Notificaciones
import pandas as pd
import holidays_co
import sys,os
import logging
import requests

Title_ = "Summary of Date"

Dic_month = {
    "01":'Enero',
    "02":'Febrero',
    "03":'Marzo',
    "04":'Abril',
    "05":'Mayo',
    "06":'Junio',
    "07":'Julio',
    "08":'Agosto',
    "09":'Septiembre',
    "10":'Octubre',
    "11":'Noviembre',
    "12":'Diciembre'
}

path_ = os.getcwd()
month_ = datetime.datetime.now().month
year_ = datetime.datetime.now().year
day_  = datetime.datetime.now().day

Folder_path = os.path.join(path_, '01_salidas','F-523',str(year_),str(month_).zfill(2)+'_'+Dic_month[str(month_).zfill(2)],str(day_).zfill(2))

#The code is creating a list of years, which includes the current year, the previous year, and the next year.
years = [(year_)-1,(year_),(year_)+1]
days_list = []
for i in years:
    days_list.append(holidays_co.get_colombia_holidays_by_year(i))

df_holidays = pd.concat([pd.DataFrame(dayly) for dayly in days_list], ignore_index=True)

if datetime.datetime.now().weekday==5 or datetime.datetime.now().weekday==6:
    SystemExit
elif datetime.datetime.now().date() in df_holidays:
    SystemExit
else:
    pass 

url = "https://www.datos.gov.co/resource/qhpu-8ixx.json"
response = requests.get(url)
status_code = response.status_code

if status_code ==200:
    pass
else:
    Notificaciones.notifications_error_Connection(url,response,523)
    SystemExit

if not os.path.exists(Folder_path):
    os.makedirs(Folder_path)

log =      Folder_path +'\\'+  str(month_) + str(day_)+str(year_)+"_F523.log"
logging.basicConfig(filename=log, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


#The code defines a function called `date_proc` that takes two parameters: `date_sys` and `days_`.
def date_proc(date_sys,days_):
    List_days = []
    list_weekend = []
    contador =1
    while len(List_days)<days_:
        if (date_sys - datetime.timedelta(days=contador)).weekday()==5 or (date_sys - datetime.timedelta(days=contador)).weekday()==6:
            list_weekend.append(date_sys - datetime.timedelta(days=contador))
        elif (date_sys - datetime.timedelta(days=contador)) in df_holidays["date"].values:
            list_weekend.append(date_sys - datetime.timedelta(days=contador))
        else:
            List_days.append(date_sys - datetime.timedelta(days=contador))
        contador = contador + 1
    return min(List_days)

#The code you provided defines a function called `dates` that takes in a parameter called `dates`.
def dates (dates):
    date_prod_f = date_proc(dates,2)
    date_prod_i = date_proc(dates,3)

    if (date_prod_f -date_prod_i).days >1:
        star = (date_prod_i +datetime.timedelta(days=1))
        end = date_prod_f
    else:
        star = date_prod_f
        end = date_prod_f

    star = datetime.date(star.year,star.month,star.day).strftime('%Y-%m-%d')
    end = datetime.date(end.year,end.month,end.day).strftime('%Y-%m-%d')
    return star, end

#The code snippet is checking the current day of the week using the `now.weekday()` function.
days_works=[0,1,2,3]
day_of_week = datetime.datetime.now().weekday()
process_date = datetime.datetime.now().date()


if day_of_week in days_works:
    start, end = dates(process_date)
    #
    logging.info(f"Fecha Proceso: {process_date}")
    logging.info(f"Fecha Corte  : {start}")
    logging.info(f"Estableciendo conexion Datos Abiertos {url}")
    #
    df_datos_abiertos = Conexion_TDTA.Connection_DatosAbiertos(start,end,url)
    logging.info(f"Los Datos fueron Extraidos exitosamente de Datos Abiertos para el corte comprendido entre {start} y {end}")
    #
    where_ = f""" dt.FECHA_CORTE between '{start}' and '{end}'
    and codigo_neg <>1
    Order by 1,2,3,4,6"""
    query = f"{Select_from}\n{where_}"
    #
    logging.info(f"Estableciendo conexion TeraData")
    conexion =Conexion_TDTA(host=hosttd,user=userdb,password=pss) 
    df_TeraData = conexion.execute_query(query=query)
    logging.info(f"Los Datos fueron Extraidos Exitosamente de Teradata para el corte comprendido entre {start} y {end}")

        
elif day_of_week==4:
    dataframes_resultantes = []
    dataframes_resultantes_da = []
    start = datetime.date(year=2016,month=1,day=1)
    end  = date_proc(datetime.datetime.now().date(),2)
    periodos = Conexion_TDTA.pares(start,end)
    logging.info(f'El periodo de datos evaluados son: {start} - {end}')
    logging.info(f"Estableciendo conexion TeraData")
    
    for i in range(len(periodos)):
        where_ = f""" dt.FECHA_CORTE between '{periodos[i][0]}' and '{periodos[i][1]}'
        and codigo_neg <>1
        Order by 1,2,3,4,6"""
        query = f"{Select_from}\n{where_}"
        conexion =Conexion_TDTA(host=hosttd,user=userdb,password=pss) 
        dataframes_resultantes.append(conexion.execute_query(query=query))
        logging.info(f'se cargaron los datos de Teradata para el periodo de {periodos[i][0]} al {periodos[i][1]}')

    logging.info(f"Estableciendo conexion Datos Abiertos {url}")   
    for i in range (len(periodos)):
        dataframes_resultantes_da.append(Conexion_TDTA.Connection_DatosAbiertos(periodos[i][0],periodos[i][1],url))
        logging.info(f'se cargaron los datos de datos Abiertos para el periodo de {periodos[i][0]} al {periodos[i][1]}')
        print(f'se cargaron los datos de datos Abiertos para el periodo de {periodos[i][0]} al {periodos[i][1]}')
    
    df_TeraData = pd.concat(dataframes_resultantes,axis=0) 
    logging.info(f'Finalizo el Proceso de Consolidacion TeraData')
    df_datos_abiertos = pd.concat(dataframes_resultantes_da, axis = 0)
    logging.info(f'Finalizo el Proceso de Consolidacion Datos Abiertos')

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


for i in Change_dtypes_DatosAbiertos.keys():
    df_datos_abiertos[i.lower()] = df_datos_abiertos[i].astype(Change_dtypes_DatosAbiertos[i])

for i in Change_dtypes_TeraData.keys():
    df_TeraData[i] = df_TeraData[i].astype(Change_dtypes_TeraData[i])

df_TeraData.fecha_corte = pd.to_datetime(df_TeraData.fecha_corte, format='%d/%m/%Y')
df_datos_abiertos.fecha_corte = pd.to_datetime(df_datos_abiertos.fecha_corte)#, format='%d/%m/%Y')

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

Df_Detalle_Diferencias['Diferencia'] =  Df_Detalle_Diferencias["cantidad_TeraData"] - Df_Detalle_Diferencias["cantidad_Datos_Abiertos"]
Df_Detalle_Diferencias["bool"] = Df_Detalle_Diferencias['Diferencia'].apply(lambda x: 0 if x == 0 else 1)
Df_Detalle_Diferencias["Validacion"] = Df_Detalle_Diferencias['Diferencia'].apply(lambda x: "Sin Diferencia" if x == 0 else "Diferencia")

logging.info(f"Cantidad de Diferenicias presentadas: {Df_Detalle_Diferencias['bool'].sum()}")

Name = Folder_path +'\\'+'Summary_by_Date_'+str(year_)+str(month_).zfill(2)+'_'+str(day_).zfill(2)
if Df_Detalle_Diferencias['bool'].sum()>0:
    Diferencias = Df_Detalle_Diferencias[Df_Detalle_Diferencias["Diferencia"]>0]
    Diferencias[["fecha_corte","cantidad_TeraData","cantidad_Datos_Abiertos","Diferencia"]].to_csv(Name, sep = '|', index=False)
    
warehouse = {
    'Observacion':['Cantidad de dias con Diferencia'],
    'Cantidad':[Df_Detalle_Diferencias['bool'].sum()],
    'Ruta': [Name]
    }

def Evaluacion(row,campo1,campo2):
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
    
Df_Detalle_Diferencias['observacion'] = Df_Detalle_Diferencias.apply(Evaluacion,axis=1,args=('cantidad_TeraData','cantidad_Datos_Abiertos'))
Df_Detalle_Diferencias.columns =['Key_', 'fecha_corte_', 'cantidad_TeraData_', 'cantidad_Datos_Abiertos_','Diferencia_', 'bool_', 'Validacion_', 'observacion_']
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

NameFile2 = Folder_path +'\\'+str(year_)+str(month_).zfill(2)+str(day_).zfill(2) + '_No_Records_TeraData.txt'
if df_No_registrados_TeraData.shape[0]>0:
    logging.warning(f"Cantidad registros que no se visualizan en TeraData: {df_No_registrados_TeraData.shape[0]}")
    logging.warning(f"El Archivo que contiene el Detalle de los registros se encuentra ubicado en: {NameFile2}")
    df_No_registrados_TeraData.to_csv(NameFile2,sep='|', index=False)
else:
    logging.info('la totalidad de registros en Teradata se reflejan en los datos cargados en Datos Abiertos')

NameFile3 = Folder_path +'\\'+str(year_)+str(month_).zfill(2)+str(day_).zfill(2) + '_No_Records_Datos_Abiertos.txt'
if df_No_registrados_Datos_Abiertos.shape[0]>0:
    logging.warning(f"Cantidad registros que no se visualizan en Datos Abiertos: {df_No_registrados_Datos_Abiertos.shape[0]}")
    logging.warning(f"El Archivo que contiene el Detalle de los registros se encuentra ubicado en: {NameFile3}")
    df_No_registrados_Datos_Abiertos.to_csv(NameFile3,sep='|', index=False)
else:
    logging.info('la totalidad de registros de datos Abiertos se registran en TeraData')

warehouse['Observacion'].append('No Registrados en TeraData'),
warehouse['Cantidad'].append(df_No_registrados_TeraData.shape[0]),
warehouse['Ruta'].append(NameFile2),
warehouse['Observacion'].append('No Registrados en Datos Abiertos'),
warehouse['Cantidad'].append(df_No_registrados_Datos_Abiertos.shape[0]),
warehouse['Ruta'].append(NameFile3)

File = pd.DataFrame(warehouse)
NameFile4 = Folder_path + "\\" + 'Summary.txt'
File.to_csv(NameFile4,sep = '|', index  =False)

if day_of_week==4:
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
    NombreArchivo4 = Folder_path +'\\'+str(year_)+str(month_).zfill(2)+str(day_).zfill(2) + '_missing_records.txt'
    Faltantes.to_csv(NombreArchivo4,sep='|',index=False)
else:
    pass

diferencia = Df_Detalle_Diferencias['bool_'].sum()

Notificaciones.notificacion_final(523,diferencia,start,end,NameFile4,log)
logging.shutdown()