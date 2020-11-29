'''
Creación de contenedores para los datos
datos1.xml  --> dataXml
datos2.json --> dataJson
datos3.csv  --> dataCsv
datos4.txt  --> dataTxt

'''
#imports
import warnings
warnings.filterwarnings("ignore")

import csv,json,xml

from xml.dom.minidom import parse, Node
import xml.etree.ElementTree as et 
from xml.etree.ElementTree import parse

import pandas as pd

#Contenedores de data
dataXml=[]
dataJson=[]
dataCsv=[]
dataTxt=[]

# CARGA DE DATOS JSON -------------------------------------------------------
with open('./data/datos2.json') as f:
    jsonReaded = json.load(f)
    for i in jsonReaded:
        print(i)
        dataJson.append(i)

json_df= pd.DataFrame(dataJson)
#renombrar columnas ['price', 'bedrooms', 'bathrooms', 'sqft_living', 'sqft_lot', 'floors']
json_df=json_df.rename(columns={'price':'price_json', 'bedrooms':'bedrooms_json', 'bathrooms':'bathrooms_json',
                                'sqft_living':'sqft_living_json', 'sqft_lot':'sqft_lot_json', 'floors':'floors_json'})
json_df.reset_index(drop=True, inplace=True)
print(json_df.sample(2))

# CARGA DE DATOS CSV --------------------------------------------------------
with open('./data/datos3.csv') as f:
    csvReaded = csv.reader(f, delimiter=",")
    for i in csvReaded:
        print(i)
        dataCsv.append(i)

csv_df= pd.DataFrame(dataCsv,columns =['price_csv', 'bedrooms_csv', 'bathrooms_csv', 'sqft_living_csv', 'sqft_lot_csv', 'floors_csv'])
csv_df=csv_df[1:]
csv_df.reset_index(drop=True, inplace=True)
print(csv_df.sample(2))

# CARGA DE DATOS TXT --------------------------------------------------------
with open('./data/datos4.txt') as f:
    txtReaded = csv.reader(f, delimiter=" ")
    for i in txtReaded:
        print(i)
        dataTxt.append(i)
        
txt_df= pd.DataFrame(dataCsv,columns =['price_txt', 'bedrooms_txt', 'bathrooms_txt', 'sqft_living_txt', 'sqft_lot_txt', 'floors_txt'])
txt_df=txt_df[1:]
txt_df.reset_index(drop=True, inplace=True)
print(txt_df.sample(2))


# CARGA DE DATOS XML --------------------------------------------------------
xml_doc = parse('./data/datos1.xml')

for ele in xml_doc.findall('bedrooms'):
    print(ele.text)
    
xtree = et.parse("./data/datos1.xml")
xmlReaded = xtree.getroot()

xmlrows=[]

for node in xmlReaded: 
    #x_row = node.attrib.get("row")
    x_price = node.find("price")
    x_bedrooms = node.find("bedrooms").text
    x_bathrooms = node.find("bathrooms").text
    x_living = node.find("sqft_living").text
    x_lot = node.find("sqft_lot").text
    x_floors = node.find("floors").text
    
    xmlrows.append({'price':x_price, 'bedrooms':x_bedrooms, 'bathrooms':x_bathrooms,
                    'sqft_living':x_living, 'sqft_lot':x_lot, 'floors':x_floors})
    
xml_df=pd.DataFrame(xmlrows,columns= ['price', 'bedrooms', 'bathrooms', 'sqft_living', 'sqft_lot', 'floors'])

xml_df['price']=json_df['price_json']

xml_df=xml_df.rename(columns={'price':'price_xml', 'bedrooms':'bedrooms_xml', 'bathrooms':'bathrooms_xml',
                                'sqft_living':'sqft_living_xml', 'sqft_lot':'sqft_lot_xml', 'floors':'floors_xml'})


xml_df.reset_index(drop=True, inplace=True)
print(xml_df.sample(2))




# ------- CONCATENACIÓN DE DATA ----------------------------------------------
datos_total=pd.concat([txt_df,csv_df,json_df,xml_df],axis=1)
print(datos_total.shape)
print(datos_total.sample(3))

# ----------- FILTRADO DE DATA ----------------------------------------------
#extracción de columnas (1 de cada dataset)
filter_data=datos_total[['price_txt','bedrooms_csv','bathrooms_json','sqft_living_xml']]

#tipos de datos asociados
print(filter_data.dtypes)

#Transformación de data tipo object a float
filter_data['price_txt'] = filter_data['price_txt'].astype(float)
filter_data['bedrooms_csv'] = filter_data['bedrooms_csv'].astype(float)
filter_data['sqft_living_xml'] = filter_data['sqft_living_xml'].astype(float)

#Muestra del resultado
print(f"\nDATOS FILTRADOS: \n{filter_data.sample(3)}\n")


# ----------- CREACIÓN DE COLUMNAS MAX MIN Y PROMEDIO ------------------------
filter_data['max']=float("NaN")
filter_data['min']=float("NaN")
filter_data['promedio']=float("NaN")
print(filter_data.sample(5))

# ----------- CALCULO DE VALORES  MAX MIN Y PROMEDIO ------------------------
maxValues=filter_data.max(axis=1)
minValues=filter_data.min(axis=1)
meanValues=filter_data.mean(axis=1)

# ----------- ASIGNACIÓN DE VALORES MAX MIN Y PROMEDIO -----------------------
filter_data['max']=maxValues
filter_data['min']=minValues
filter_data['promedio']=meanValues
print("\n CALCULO DE VALORES MÁXIMO - MÍNIMO - PROMEDIO POR FILA\n")
print(filter_data)

# ----------- CALCULO DE VALORES MAX MIN Y PROMEDIO POR COLUMNA --------------
print("\n CALCULO DE VALORES MAX MIN Y PROMEDIO POR COLUMNA\n")
print(f"Valor máximo:{filter_data['max'].max()}\n")
print(f"Valor mínimo:{filter_data['min'].min()}\n")
print(f"Valor promedio:{filter_data['promedio'].mean()}\n")



# ----------- EXPORTAR LOS RESULTADOS OBTENIDOS ------------------------------

#CSV
filter_data.to_csv(r'./newData/filter_data3.csv')

#JSON
filter_data.to_json(r'./newData/filter_data2.json',orient='split')

#TXT
with open('./newData/filter_data4.txt', 'w') as f: filter_data.to_string(f, col_space=1)

#XML
def to_xml(df, filename=None, mode='w'):
    
    def row_to_xml(row):
        xml = ['<item>']
        for i, col_name in enumerate(row.index):
            xml.append('  <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
        xml.append('</item>')
        return '\n'.join(xml)
    res = '\n'.join(df.apply(row_to_xml, axis=1))

    if filename is None:
        return res
    with open(filename, mode) as f:
        f.write(res)
        
to_xml(filter_data,'./newData/filter_data1.xml')