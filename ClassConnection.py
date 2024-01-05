import teradatasql
import pandas as pd
import requests
import datetime
import logging


class Conexion_TDTA:

    def __init__(self, host:str, user:str, password:str):
        """
        stores the parameters to establish a connection to the database

        parameters:
        host (str): hostname 
        user (str): user of Database
        password (str): password of database

        """
        self.host = host
        self.user = user
        self.password = password
 

    def excecute_query(self, query:str):

        """
        execute query of SQL and return a DataFrame

        parameters:

        Query (str): query of Database Teradata
        
        Returns:
        DataFrame
        """
        try:
            conn = teradatasql.connect(host=self.host, user=self.user, password=self.password)       
            df = pd.read_sql(query, conn,)
            conn.close()
            return True,df

        except Exception as error:       
            logging.warning(f"Error de conexión: {str(error)}")
            return False, None
        

    def Connection_DatosAbiertos(start:str,end:str, url:str):
        """
        establishing a connection to Datos Abiertos

        parameters:

        start (str): start day of query 'yyyy-mm-dd'
        end (str): end day of query 'yyyy-mm-dd'
        url (str): URL of API

        Returns:
        DataFrame
        """
        
        all_data = []
        start = f"{start}T00:00:00"
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
    

    def pares(date_min:datetime, date_max:datetime):
        """
        parameters:

        date_min (datetime): start day  
        date_max (datetime): end day 
        
        Returns:
        list
        """        
        fechas = []
        lista_par = []

        fecha_actual = date_min

        while date_min <= date_max:
            fechas.append(date_min.strftime('%Y-%m-%d'))
            
            # Cambiar entre el 1 de enero y el 31 de diciembre
            if date_min.month == 1:
                date_min = date_min.replace(month=12, day=31, year=date_min.year)
            else:
                date_min = date_min.replace(month=1, day=1, year=date_min.year + 1)

        # Agregar la fecha de fin si es posterior a la última fecha generada
        if date_min > date_max:
            fechas.append(date_max.strftime('%Y-%m-%d'))

        # Obtener parejas ordenadas
        parejas = [(fechas[i], fechas[i + 1]) for i in range(0, len(fechas) - 1, 2)]

        # Imprimir las parejas
        for pareja in parejas:
            lista_par.append(pareja)
        return lista_par


