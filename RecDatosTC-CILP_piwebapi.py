import numpy as np
import pandas as pd
from gekko import GEKKO

import datetime
import pytz

import os
import requests
from requests.auth import HTTPBasicAuth
import base64
import json

import warnings
warnings.filterwarnings("ignore")



def call_headers(include_content_type):
    """ Create API call headers
        @includeContentType boolean: Flag determines whether or not the
        content-type header is included
    """
    if include_content_type is True:
        header = {
            'content-type': 'application/json',
            'X-Requested-With': 'XmlHttpRequest'
        }
    else:
        header = {
            'X-Requested-With': 'XmlHttpRequest'
        }

    return header



def read_PI(tag, start_date, end_date, mode):
 
    security_auth = HTTPBasicAuth("YS01480","2gY7akMz")
    x = "https://swplpglpapl15/piwebapi/attributes?path=" #PIWEBAPI
    y = "\\\\PIRLP\\" #DataArchive
    z = tag #Tag

    df_out = pd.DataFrame()

    getWebId= x+y+z 
    response = requests.get(getWebId, auth=security_auth, verify=False)
    convertido_json = response.json()

    if response.status_code == 200:
        webId = convertido_json["WebId"]
        
        if mode == 'recorded_data':
            RecordedData = convertido_json["Links"]["RecordedData"] #aca defino que tipo de Dato quiero
        elif mode == 'current_value':
            RecordedData = convertido_json["Links"]["Value"]
            
    else:
        print(response.status_code)
    concat = '?startTime='+start_date+'&endTime='+end_date #depende de fechas literales o relativas
    
    #Incluir fechas si estamos pidiendo recorded data; caso contrario no considerar el tiempo
    if mode == 'recorded_data':
        salida = RecordedData+concat
    elif mode == 'current_value':
        salida = RecordedData

    response = requests.get(salida, auth=security_auth,verify=False, timeout=(120, 120))
    data_json = response.json()
    cantElem= str(data_json.items()).count('Value')
    
    if mode == 'recorded_data':
        df_aux = pd.DataFrame(data_json['Items'])
        df_aux = df_aux[['Timestamp','Value']]
        df_out = pd.concat([df_out,df_aux],ignore_index=True)

        while cantElem > 999:
            start_date = df_aux['Timestamp'].iat[-1] #Ultimo TimeStamp
            concat= '?startTime='+start_date+'&endTime='+end_date #depende de fechas literales o relativas
            salida = RecordedData+concat
            response = requests.get(salida, auth=security_auth,verify=False, timeout=(120, 120))
            data_json = response.json()
            cantElem= str(data_json.items()).count('Value')
            df_aux = pd.DataFrame(data_json['Items'])
            df_aux = df_aux[['Timestamp','Value']]
            df_out = pd.concat([df_out,df_aux],ignore_index=True)
            
        df_out1 = []
        for s in df_out['Value']:
            if type(s) == float:
                df_out1.append(s)

        if len(df_out1) > 0:
            val = np.mean(df_out1)
        else:
            val = 1
            
    elif mode == 'current_value':
        if type(data_json['Value']) == float:       
            val = data_json['Value'] #ya tiene el current value
            
        else:
            if tag[-3:] == 'Err':
                val = 1e+12
            else:
                val = 1

    #Agregar código para eliminar valores no numéricos
    #Si no hay ningún valor numérico, devolver 1 para inicializar los cálculos y establecer error = 1E+12
    
    return val


def write_PI(tag, val):
    
    x = "https://swplpglpapl15/piwebapi/attributes?path=" #PIWEBAPI
    y = "\\\\PIRLP\\" #DataArchive
    z = tag #Tag
   
    getWebId= x+y+z 
    
    #  create security method - basic or kerberos
    security_method = HTTPBasicAuth("YS01480","2gY7akMz")

    #  Get the sample tag
    response = requests.get(getWebId, auth=security_method, verify=False) #verify=False

    #  Only continue if the first request was successful
    if response.status_code == 200:
        #  Deserialize the JSON Response
        data = json.loads(response.text)

        #  Create the data for this call
        data_value = val
        request_body = {
            'Value': data_value
        }

        #  Create the header
        header = call_headers(True)

        #  Write the single value to the tag
        response = requests.post(data['Links']['Value'], auth=security_method,
                                 verify=False, json=request_body, headers=header)

        if response.status_code == 202:
            print('Attribute SampleTag write value ' + str(data_value))
        else:
            print(response.status_code, response.reason, response.text)
    else:
        print(response.status_code, response.reason, response.text)
        
    return response.status_code




#Leer tags para datos medidos
df1 = pd.read_csv('tagsTC.csv')
tags_TC = list(df1.columns.values)
#tags_TC = np.array(tags_TC)

#Leer tags para desvío de instrumentos
tags_desvTC = df1.values[2,:]

#Leer tags para densidades de laboratorio - generar el vector de tags "in situ", ya que pandas no puede leer el caracter "º"
tags_dlabTC = np.array(['CC-xTC_Carga_Densidad15°C', 'CC-xTC_CrudoEntradaDesalador_Densidad15°C', 'CC-xTC_CRU_SAL_DES_A_E2_DENSIDAD_15', 'CC-xTC_GasResidual_Densidad', 'CC-xTC_NaftaReflujo_Densidad15°C', 'CC-xTC_EntradaEstabilizadora_Densidad15°C', 'CC-xTC_NaftaEstabilizada_Densidad15°C', 'CC-xTC_NaftaLiviana_Densidad15°C', 'CC-xTC_NaftaPesada_Densidad15°C', 'CC-xTC_Kerosene_Densidad15°C', 'CC-xTC_GasOilLiviano_Densidad15°C', 'CC-xTC_GasOilPesado_Densidad15°C', 'CC-xTC_CrudoReducido_Densidad15°C'])

#Matriz para balances de materia
df5 = pd.read_csv('matBM-TC.csv')
mat_TC = df5.values #TC

#Densidades TC
dens_TC = df1.values[3,:]
dens_TC = dens_TC.astype(np.float64)
dens_TC = dens_TC.reshape(1,dens_TC.shape[0])


# Usando piwebapi
t = 1
while t < 10:

    #Inicializar vectores para datos de PI
    xm_TC = np.zeros(len(tags_TC))
    err_TC = np.zeros(len(tags_TC))
    dens_labTC = np.zeros(tags_dlabTC.shape[0])


    #Leer datos medidos y almacenar en xm_TC
    i = 0
    for tag in tags_TC:

        if tag == 'Inferencia' or tag == 'Finf_GOL' or tag == 'Tinf_crudo2' or tag == 'Tinf_crudo1' or tag == 'Tinf_crudo4' or tag == 'Tinf_CR' or tag == 'Tinf_crudo3':
            xm_TC[i] = 1
        else:
            xm_TC[i] = read_PI(str(tag), '*-1h', '*', 'recorded_data')

        i = i + 1


    #Leer errores instrumentales
    i = 0
    for tag in list(tags_desvTC):

        if tag == 'Inferencia_Err' or tag == 'TC-GOLDA803_Err' or tag == 'TC-CRUDOEA803_Err' or tag == 'TC-CRUDOEA825_Err' or tag == 'TC-CRUDOEA805_Err' or tag == 'TC-CRUDOEA801_Err' or tag == 'TC-CRUDOEA802_Err':
            err_TC[i] = 1e+12
        else:
            err_TC[i] = read_PI(str(tag), '*-1h', '*', 'current_value')

        i = i + 1



    #Leer densidades de laboratorio
    i = 0
    for tag in list(tags_dlabTC):

        if i != 0 and i != 3 and i != 4 and i != 5 and i != 6:
            dens_labTC[i] = read_PI(str(tag), '*-30d', '*', 'recorded_data')#Promedio últimos 30 días

        else: #Corrientes que se analizan esporádicamente
            dens_labTC[i] = read_PI(str(tag), '*-1h', '*', 'current_value')#Current Value

        i = i + 1




    #Redimensionar a vectores fila
    xm_TC = xm_TC.reshape(1,xm_TC.shape[0])
    err_TC = err_TC.reshape(1,err_TC.shape[0])


    #Matrices para balances de materia
    df5 = pd.read_csv('matBM-TC.csv')
    mat_TC = df5.values #TC

    #Densidades TC
    dens_TC = df1.values[3,:]
    dens_TC = dens_TC.astype(np.float64)
    dens_TC = dens_TC.reshape(1,dens_TC.shape[0])

    #Sobreescribir densidades lab TC
    for s in range(dens_TC.shape[1]):
        if s==0 or s==2 or s==3 or s==5 or s==6 or s==10: #Crudo carga desalador
            dens_TC[0,s] = dens_labTC[1]
        if s==8 or s==9 or s==13 or s==14 or s==19 or s==20 or s==21 or s==22 or s==23 or s==24 or s==25 or s==26: #Crudo salida desalador
            dens_TC[0,s] = dens_labTC[2]
        elif s==32 or s==42: #NL a estabilizadora
            dens_TC[0,s] = dens_labTC[5]
        elif s==44: #LPG
            dens_TC[0,s] = dens_labTC[3]
        elif s==45: #NL
            dens_TC[0,s] = dens_labTC[7]
        elif s==33: #NP
            dens_TC[0,s] = dens_labTC[8]
        elif s==34: #Kero
            dens_TC[0,s] = dens_labTC[9]
        elif s==35 or s==36 or s==37: #GOL
            dens_TC[0,s] = dens_labTC[10]
        elif s==46 or s==47 or s==48: #GO circulante
            dens_TC[0,s] = 0.820
        elif s==38 or s==39: #GOP
            dens_TC[0,s] = dens_labTC[11]
        elif s==40 or s==41 or s==49 or s==50 or s==51 or s==52 or s==53: #CR
            dens_TC[0,s] = dens_labTC[12]

    #Cálculo de caudales másicos
    xm_TC = xm_TC * dens_TC

    #Asegurar que no haya valores nulos en el cálculo de la función objetivo
    for i in range(xm_TC.shape[1]):
        if xm_TC[0,i] <= 0.05:
            xm_TC[0,i] = 0.05


    recdata_TC = np.zeros(xm_TC.shape)
    FO_TC = np.zeros(recdata_TC.shape[0])
    error_TC = np.zeros(recdata_TC.shape)
    sigma_TC = np.zeros(recdata_TC.shape)



    #Inicializar Gekko, definir vectores variables y establecer opciones
    m = GEKKO(remote=False)
    m.options.MAX_ITER = 3000

    x_TC = m.Array(m.Var,(xm_TC.shape[1]))

    i = 0
    j = 0
    for xi in x_TC:
        xi.value = xm_TC[i,j]
        xi.lower = 0
        xi.upper = 700
        j+= 1


    #Balances de materia TC
    m.Equation(np.sum(mat_TC[:,0]*x_TC[0:54]) == 0) #Crudo carga - EA800/880
    m.Equation(np.sum(mat_TC[:,1]*x_TC[0:54]) == 0) #Crudo EA801
    m.Equation(np.sum(mat_TC[:,2]*x_TC[0:54]) == 0) #Crudo EA804
    m.Equation(np.sum(mat_TC[:,3]*x_TC[0:54]) == 0) #Carga desalador
    m.Equation(np.sum(mat_TC[:,4]*x_TC[0:54]) == 0) #Agua desalador
    m.Equation(np.sum(mat_TC[:,5]*x_TC[0:54]) == 0) #Desaladores
    m.Equation(np.sum(mat_TC[:,6]*x_TC[0:54]) == 0) #Horno ramal A
    m.Equation(np.sum(mat_TC[:,7]*x_TC[0:54]) == 0) #Horno ramal B
    m.Equation(np.sum(mat_TC[:,8]*x_TC[0:54]) == 0) #Efluente desalador
    m.Equation(np.sum(mat_TC[:,9]*x_TC[0:54]) == 0) #Fraccionadora
    #m.Equation(np.sum(mat_TC[:,10]*x_TC[0:54]) == 0) #Estabilizadora NL - se eliminó por problemas en mediciones gases, LPG y NL
    m.Equation(np.sum(mat_TC[:,11]*x_TC[0:54]) == 0) #CR EA807
    m.Equation(np.sum(mat_TC[:,12]*x_TC[0:54]) == 0) #CR EA801
    m.Equation(np.sum(mat_TC[:,13]*x_TC[0:54]) == 0) #CR salida
    m.Equation(np.sum(mat_TC[:,14]*x_TC[0:54]) == 0) #GO circulante


    #Balances de energía TC

    #Crudo - GOL circulante
    #EA825- EA803
    m.Equation(x_TC[48]/(x_TC[2]+x_TC[3])*(x_TC[55]-x_TC[56])/(x_TC[57]-x_TC[75]) - x_TC[48]/(x_TC[5]+x_TC[6]+x_TC[7])*(x_TC[59]-x_TC[55])/(x_TC[61]-x_TC[60]) == 0)
    #EA803- EA804A
    m.Equation(1/(x_TC[48]/(x_TC[5]+x_TC[6]+x_TC[7])*(x_TC[59]-x_TC[55])/(x_TC[61]-x_TC[60])) - 1/(x_TC[46]/x_TC[8]*(x_TC[63]-x_TC[64])/(x_TC[65]-x_TC[58])) == 0)
    #EA804A- EA804B
    m.Equation(x_TC[46]/x_TC[8]*(x_TC[63]-x_TC[64])/(x_TC[65]-x_TC[58]) - x_TC[47]/x_TC[9]*(x_TC[63]-x_TC[66])/(x_TC[67]-x_TC[58]) == 0)
    #GOL circulante EA804
    m.Equation(x_TC[46]*(x_TC[59]-x_TC[64]) + x_TC[47]*(x_TC[59]-x_TC[66]) == 0)
    #Crudo EA804
    m.Equation(x_TC[8]*(x_TC[76]-x_TC[65]) + x_TC[9]*(x_TC[76]-x_TC[67]) == 0)

    #Crudo - GOL
    #EA880 - EA805
    m.Equation((x_TC[35]+x_TC[36]+x_TC[37])/x_TC[3]*(x_TC[68]-x_TC[69])/(x_TC[70]-x_TC[71]) - (x_TC[35]+x_TC[36]+x_TC[37])/(x_TC[13]+x_TC[14])*(x_TC[62]-x_TC[74])/(x_TC[78]-x_TC[76]) == 0)
    #EA805 - EA802
    m.Equation((x_TC[35]+x_TC[36]+x_TC[37])/(x_TC[13]+x_TC[14])*(x_TC[62]-x_TC[74])/(x_TC[78]-x_TC[76]) - (x_TC[35]+x_TC[36]+x_TC[37])/(x_TC[5]+x_TC[6])*(x_TC[74]-x_TC[77])/(x_TC[60]-x_TC[101]) == 0) #EA802
    #Crudo EA800/880
    m.Equation(x_TC[2]*(x_TC[75]-x_TC[72]) + x_TC[3]*(x_TC[75]-x_TC[70]) == 0)

    #Crudo - CR
    #EA807A - EA807B
    m.Equation(x_TC[40]/x_TC[13]*(x_TC[82]-x_TC[83])/(x_TC[84]-x_TC[85]) - x_TC[41]/x_TC[14]*(x_TC[82]-x_TC[90])/(x_TC[91]-x_TC[92]) == 0)
    #EA807B - EA807C
    m.Equation(x_TC[41]/x_TC[14]*(x_TC[82]-x_TC[90])/(x_TC[91]-x_TC[92]) - x_TC[40]/x_TC[13]*(x_TC[83]-x_TC[86])/(x_TC[85]-x_TC[87]) == 0)
    #EA807C - EA807D
    m.Equation(x_TC[40]/x_TC[13]*(x_TC[83]-x_TC[86])/(x_TC[85]-x_TC[87]) - x_TC[41]/x_TC[14]*(x_TC[90]-x_TC[93])/(x_TC[92]-x_TC[94]) == 0)
    #EA807D - EA807E
    m.Equation(x_TC[41]/x_TC[14]*(x_TC[90]-x_TC[93])/(x_TC[92]-x_TC[94]) - x_TC[40]/x_TC[13]*(x_TC[86]-x_TC[88])/(x_TC[87]-x_TC[89]) == 0)
    #EA807E - EA807F
    m.Equation(x_TC[40]/x_TC[13]*(x_TC[86]-x_TC[88])/(x_TC[87]-x_TC[89]) - x_TC[41]/x_TC[14]*(x_TC[93]-x_TC[95])/(x_TC[94]-x_TC[89]) == 0)
    #EA807F - EA801A/C
    m.Equation(1/(x_TC[41]/x_TC[14]*(x_TC[93]-x_TC[95])/(x_TC[94]-x_TC[89])) - 1/(x_TC[49]/x_TC[5]*(x_TC[96]-x_TC[97])/(x_TC[79]-x_TC[98])) == 0)
    #EA801A/C - EA801B/D
    m.Equation(x_TC[49]/x_TC[5]*(x_TC[96]-x_TC[97])/(x_TC[79]-x_TC[98]) - x_TC[50]/x_TC[6]*(x_TC[96]-x_TC[99])/(x_TC[80]-x_TC[100]) == 0)
    #CR EA807
    m.Equation(x_TC[40]*(x_TC[96]-x_TC[88]) + x_TC[41]*(x_TC[96]-x_TC[95]) == 0)
    #Crudo EA801
    m.Equation(x_TC[5]*(x_TC[101]-x_TC[79]) + x_TC[6]*(x_TC[101]-x_TC[80]) ==0)


    #Restricciones delta T (TC)

    #Crudo-GOL circulante (EA825, EA803, EA804 A/B)
    m.Equation(x_TC[55]-x_TC[56] >= 1)
    m.Equation(x_TC[57]-x_TC[75] >= 1)
    m.Equation(x_TC[59]-x_TC[55] >= 1)
    m.Equation(x_TC[61]-x_TC[60] >= 1)
    m.Equation(x_TC[63]-x_TC[64] >= 1)
    m.Equation(x_TC[65]-x_TC[58] >= 1)
    m.Equation(x_TC[63]-x_TC[66] >= 1)
    m.Equation(x_TC[67]-x_TC[58] >= 1)

    #Crudo-GOL (EA880, EA805, EA802)
    m.Equation(x_TC[68]-x_TC[69] >= 1)
    m.Equation(x_TC[70]-x_TC[71] >= 1)
    #m.Equation(x_TC[73]-x_TC[74] >= 1)
    m.Equation(x_TC[78]-x_TC[76] >= 1)
    m.Equation(x_TC[74]-x_TC[77] >= 1)
    m.Equation(x_TC[60]-x_TC[101] >= 1)

    #Crudo-CR (EA807 A-F, EA801 A-D)
    m.Equation(x_TC[82]-x_TC[83] >= 1)
    m.Equation(x_TC[84]-x_TC[85] >= 1)
    m.Equation(x_TC[82]-x_TC[90] >= 1)
    m.Equation(x_TC[91]-x_TC[92] >= 1)
    m.Equation(x_TC[83]-x_TC[86] >= 1)
    m.Equation(x_TC[85]-x_TC[87] >= 1)
    m.Equation(x_TC[90]-x_TC[93] >= 1)
    m.Equation(x_TC[92]-x_TC[94] >= 1)
    m.Equation(x_TC[86]-x_TC[88] >= 1)
    m.Equation(x_TC[87]-x_TC[89] >= 1)
    m.Equation(x_TC[93]-x_TC[95] >= 1)
    m.Equation(x_TC[94]-x_TC[89] >= 1)
    m.Equation(x_TC[96]-x_TC[97] >= 1)
    m.Equation(x_TC[79]-x_TC[98] >= 1)
    m.Equation(x_TC[96]-x_TC[99] >= 1)
    m.Equation(x_TC[80]-x_TC[100] >= 1)

    #Restricciones inferencias EA805
    m.Equation(x_TC[89]-x_TC[78] >= 1)
    m.Equation(x_TC[73]-x_TC[62] >= 1)
    m.Equation(x_TC[62]-x_TC[74] >= 1)


    #Función objetivo TC
    e1_TC = np.sum(((xm_TC[i,0:55]-x_TC[0:55])/xm_TC[i,0:55]/err_TC[0,0:55])**2)
    e2_TC = np.sum(((xm_TC[i,55:]-x_TC[55:])/err_TC[0,55:])**2)

    m.Minimize(e1_TC + e2_TC)

    #Ejecutar optimización
    try:
        m.solve(disp=False)

        #Resultados:
        for n in range(recdata_TC.shape[1]):
            recdata_TC[i,n] = x_TC[n].value[0]
            if n <= 54:
                error_TC[i][n] = ((xm_TC[i,n]-recdata_TC[i,n])/xm_TC[i,n]/err_TC[0,n])**2
                sigma_TC[i][n] = (xm_TC[i,n]-recdata_TC[i,n])/xm_TC[i,n]/err_TC[0,n]
                recdata_TC[i,n] = x_TC[n].value[0]/dens_TC[0,n]
            elif n >= 55: #and n<= 125:
                error_TC[i][n] = ((xm_TC[i,n]-recdata_TC[i,n])/err_TC[0,n])**2
                sigma_TC[i][n] = (xm_TC[i,n]-recdata_TC[i,n])/err_TC[0,n]

        FO_TC[i] = np.sum(error_TC[i,:])

        t = 11

    except Exception as e:
        print("no converge")
        print(e)
        #Si la optimización no converge, escribir 'Bad' en PI para todos los tags
        n=0
        recdata_TC = recdata_TC.astype(np.str_)
        sigma_TC = sigma_TC.astype(np.str_)
        FO_TC = FO_TC.astype(np.str_)
        for n in range(recdata_TC.shape[1]):
            recdata_TC[i,n] = 'Bad'
            sigma_TC[i,n] = 'Bad'
        FO_TC[i] = 'Bad'
    
    
    
    
#Transformar vectores de resultados a listas para poder escribir en PI
#recdata_TC = recdata_TC.tolist()
#sigma_TC = sigma_TC.tolist()
#FO_TC = FO_TC.tolist()

#Cargar TAGs para valores reconciliados. Ídem sigmas.
tags_recTC = list(df1.values[0,:])
tags_sigmaTC = list(df1.values[1,:])
# MG reemplazo alias PIRLP por nombre del server



#Escribir datos reconciliados en PI
i = 0
for tag in tags_recTC:
    print(tag)
    write_PI(str(tag), recdata_TC[0][i])
    i = i + 1
    
    
#Escribir valores de sigma en PI
i = 0
for tag in tags_sigmaTC:
    print(tag)
    write_PI(str(tag), sigma_TC[0][i])
    i = i + 1
    
    
#Escribir función objetivo en PI
write_PI('TC-RECDATOS_FO', FO_TC[0])