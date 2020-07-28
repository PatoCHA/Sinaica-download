import pandas as pd
import re
import datetime
import requests
from bs4 import BeautifulSoup
import json

def wrapersinaica(estacion,sensor,fecha):
    rango= 4
    conta = sensor
    url='https://sinaica.inecc.gob.mx/pags/datGrafs.php'
    params ={'estacionId':str(estacion),'param':str(conta),'fechaIni':str(fecha),'rango':str(rango),'tipoDatos':''}

    response=requests.post(url, data=params)
    soup1 = BeautifulSoup(response.text, 'lxml')
    reportes = soup1.find_all('script')[2]
    r = str(reportes)
    r1 = r.split(' dat = ') #extraccion de datos
    r2 = r1[1].rsplit(';\n\n\t\tif(dat.length == 0)') #extraccion de datos
    r3 = r2[0].rsplit("['(.*?)']") #extraccion de datos
    JsonText=r3[0] #extraccion de datos
    return JsonText

def datosorganizar(JsonText,sensor,estacion):
    Estacion=pd.read_json(JsonText) #crear data frame con JsonText
    Estacion['sensor'] = sensor 
    Estacion['estacion'] = estacion
    Estacion['fechahora']= 0
    #print(Estacion)
    for index, row in Estacion.iterrows():
        Estacion['fechahora'][index]=datetime.datetime.strptime(row['fecha'],"%Y-%m-%d") + pd.tseries.offsets.Timedelta(hours=row['hora'])
        #print(row['fechahora'], row['hora'])
    #Cambiar nombre de valor a sensor
    Estacion.rename(columns={'valor':sensor},inplace=True)
    return Estacion


def main(t0,Estaciones,sensores):
    Dates =[]
    u = datetime.datetime.strptime(t0,"%Y-%m-%d")# YYYY-MM-DD
    hoy = datetime.datetime.today()
    while u <= hoy :
        Dates.append(u.strftime('%Y-%m-%d'))
        u = u +datetime.timedelta(days = 28) #sumarle 4 semanas
    #La lista quedo guardada en Dates

    for es in Estaciones:
        print('id estacion'+ str(es))
        sensor_dict = {} #para guardar como variables dinamicas los sensores de cada estacion.

        for s in sensores:
            sensor_dict[s] = pd.DataFrame(columns=['id', 'fecha', 'hora', s, 'bandO', 'val', 'sensor', 'estacion',
            'fechahora'])
            sensor_dict[s]
            for d in Dates:
                sensor_dict[s]=sensor_dict[s].append(datosorganizar(wrapersinaica(es,s,d),s,es))#hay que agarrarlo de la lista
                #print(d)

        Junta=pd.DataFrame(columns=sensores) #crear columnas de lista en diccionario
        for n in sensor_dict:
            print(n)
            for index, row in sensor_dict[n].iterrows():
                Junta.loc[row['fechahora'], n] = row[n] #.loc no deberia usarse asi, muy ineficiente.
        Junta.to_csv(str(es)+'.csv')

# __name__ 
if __name__=="__main__": 
    main() 
