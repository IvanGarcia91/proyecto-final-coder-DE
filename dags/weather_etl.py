import requests
import datetime as dt
import json
import pandas as pd
from sqlalchemy import create_engine 
from airflow.models import Variable

import smtplib



def etl_data():

    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    #api_key = "99d7b2c4f0d060b28c1a933b2af566a5"
    api_key=Variable.get("API_KEY")

    #ciudades de donde queremos sacar info.
    cities = ["Buenos Aires", "Mendoza", "Corrientes", "Salta", "Córdoba"]

    #calls

    w_descps=[]
    temps=[]
    feels=[]
    t_min=[]
    t_max=[]
    humdt=[]
    sunrise_t=[]
    sunset_t=[]
    w_speed=[]

    for city in cities:
        url = base_url + "appid=" + api_key + "&q=" + city

        response = requests.get(url).json()
        w_descps.append(response["weather"][0]["description"])
        temps.append(round(response["main"]["temp"] - 273.5, 2))
        feels.append(round(response["main"]["feels_like"] - 273.5, 2))
        t_min.append(round(response["main"]["temp_min"] - 273.5, 2))
        t_max.append(round(response["main"]["temp_max"] - 273.5, 2))
        humdt.append(response["main"]["humidity"])
        sunrise_t.append(dt.datetime.utcfromtimestamp(response["sys"]["sunrise"] + response["timezone"]))
        sunset_t.append(dt.datetime.utcfromtimestamp(response["sys"]["sunset"] + response["timezone"]))
        w_speed.append(response["wind"]["speed"])

    #diccionario para alamacenar la data extraida

    data_dict = {"city": cities, "weather_description":w_descps,"temperature":temps, "feels_like":feels ,"temp_min":t_min,
                "temp_max":t_max, "humidity": humdt, "wind_speed":w_speed, "sunrise":sunrise_t, "sunset":sunset_t}

    # crear df y obtener las columnas necesarios para insertar en la tabla

    df = pd.DataFrame.from_dict(data_dict)

    # Agregar la fecha del pronostico
    day = dt.date.today()
    day = pd.to_datetime(day)
    df["day"] = day

    #conexion
    try:
        conn = create_engine('postgresql://ivang4163_coderhouse:pSD0f35oxG@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')
        print("Conectado a Redshift con éxito!")

    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
    
    df.to_sql('weather', conn, index=False, if_exists='append')
    df = df.to_json()


    return df


def send_email():
    try:
        mail_user =Variable.get("SECRET_EMAIL")
        mail_pwd = Variable.get("SECRET_PWD_EMAIL")
        d_mail = Variable.get("D_EMAIL") #mail destinatario

        subject='Carga de datos'
        body_text = 'Los datos fueron cargados a la base de datos exitosamente.'
        
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(mail_user,mail_pwd) 
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(mail_user,d_mail,message)
        print('El email fue enviado correctamente.')

    except Exception as exception:
        print(exception)
        print('El email no se pudo enviar.')

