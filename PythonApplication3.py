import psycopg2
import pandas as pd
import re
import xml.etree.ElementTree as ET

def contains_arabic(text):
    arabic_re = re.compile('[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+')
    return bool(arabic_re.search(text)) if isinstance(text, str) else False

# Database connection parameters
db_params = {
    "host": "172.17.1.53",
    "database": "oscar_v12",
    "user": "bi_aziza",
    "password": "Aziza@BI2020",
    "port": "5432"
}

# SQL query to fetch data
query = """
SELECT x.x_cin, x.x_num_tel, x.x_nom, x.x_prenom, x.x_state, x.x_date_de_naissance, 
       x.x_num_contrat, x.x_civilite, LEFT(x.x_ville, 50) AS x_ville, x.x_gouv,
       s.id, s.code 
FROM x_ct_aziza_mobile x
LEFT JOIN public.oscar_site s ON x.x_magasin = s.id
WHERE x.x_state='traite'
AND Cast(x.create_date as date) >= Cast(Current_date  as date)
"""

# Mapping dictionaries
gouv_codepostal_mapping = {
    'ariana': '2080', 'beja': '9000', 'benarous': '2013', 'bizerte': '7000',
    'gabes': '6000', 'gafsa': '2100', 'jendouba': '8100', 'kairouan': '3100',
    'kasserine': '1253', 'kebeli': '4200', 'manouba': '2010', 'kef': '7100',
    'mahdia': '5100', 'medenine': '4100', 'monastir': '5000', 'nabeul': '8000',
    'sfax': '3000', 'sidibouzid': '9100', 'siliana': '6100', 'sousse': '4000',
    'tataouine': '3200', 'tozeur': '2200', 'tunis': '1000', 'zaghouana': '1100'
}

gouv_adresseid_mapping = {
    'ariana': '1', 'beja': '2', 'benarous': '3', 'bizerte': '4',
    'gabes': '5', 'gafsa': '6', 'jendouba': '7', 'kairouan': '8',
    'kasserine': '9', 'kebeli': '10', 'manouba': '11', 'kef': '12',
    'mahdia': '13', 'medenine': '14', 'monastir': '15', 'nabeul': '16',
    'sfax': '17', 'sidibouzid': '18', 'siliana': '19', 'sousse': '20',
    'tataouine': '21', 'tozeur': '22', 'tunis': '23', 'zaghouana': '24'
}

def process_data(df):
    # Replace Arabic text with None
    df = df.applymap(lambda x: None if contains_arabic(x) else x)
    
    # Fill null values with default values
    df.fillna({
        'x_cin': 0, 'x_num_tel': 0, 'x_nom': 0, 'x_prenom': 0, 
        'x_state': 0, 'x_civilite': 'MR', 'x_ville': 'TUNIS', 
        'x_gouv': 0
    }, inplace=True)
    
    # Convert 'x_date_de_naissance' to datetime
    df['x_date_de_naissance'] = pd.to_datetime(df['x_date_de_naissance'], errors='coerce')
    
    # Add day, month, and year columns from 'x_date_de_naissance'
    df['day'] = df['x_date_de_naissance'].dt.day.fillna(0).astype(int)
    df['month'] = df['x_date_de_naissance'].dt.month.fillna(0).astype(int)
    df['year'] = df['x_date_de_naissance'].dt.year.fillna(0).astype(int)

    df['x_civilite'] = df['x_civilite'].str.upper()
    
    # Transform 'code' to 'CodeMagasin'
    df['CodeMagasin'] = df['code'].apply(
        lambda x: '1308' if x in ['2000', '2001'] or (isinstance(x, str) and x.startswith('Al')) else x
    ).fillna('0')
    
    # Map 'x_gouv' to 'CodePostal' and 'AdresseID'
    df['CodePostal'] = df['x_gouv'].map(gouv_codepostal_mapping).fillna(0)
    df['AdresseID'] = df['x_gouv'].map(gouv_adresseid_mapping).fillna(0)
    
    return df

def create_xml(df, output_path):
    root = ET.Element("customerListType")
    
    for index, row in df.iterrows():
        values = ET.SubElement(root, "values")
        
        # Add child elements for each row in the DataFrame
        elements = {
            "id": row['x_num_tel'],
            "externalCodes": {
                "typeCode": "CIN",
                "code": row['x_cin'],
                "main": "false"
            },
            "person": {
                "gender": row['x_civilite'],
                "firstName": row['x_prenom'],
                "lastName": row['x_nom'],
                "birthInfos": {
                    "day": row['day'],
                    "month": row['month'],
                    "year": row['year']
                }
            },
            "issuedInStore": row['id'],
            "issuedInSupport": {
                "nature": "S",
                "id": row['id']
            },
            "issuedInBu": "11",
            "emails": {
                "custId": row['x_num_tel'],
                "mediaTypeCode": "MAIL",
                "mediaType": {
                    "code": "MAIL",
                    "nature": "M",
                    "common": "true",
                    "updatable": "true",
                    "desc": "true",
                    "scoring": "true",
                    "bounces": "true",
                    "optin": "true",
                    "favoredContact": "true",
                    "main": "true",
                    "phone": "false",
                    "web": "false",
                    "mail": "true",
                    "highwayNum": "false",
                    "additionalNum": "false",
                    "highwayType": "false",
                    "highwayLabel": "false",
                    "addressLine2": "false",
                    "addressLine3": "false",
                    "addressLine5": "false",
                    "postalCode": "false",
                    "city": "false",
                    "country": "false",
                    "territory": "false",
                    "colorCode": "6750207",
                    "moduleCode": "CRM"
                },
                "email": "Oscar@aziza.tn"
            },
            "phones": {
                "custId": row['x_num_tel'],
                "mediaTypeCode": "TELM",
                "mediaType": {
                    "code": "TELM",
                    "nature": "G",
                    "common": "true",
                    "updatable": "true",
                    "desc": "true",
                    "scoring": "true",
                    "bounces": "true",
                    "optin": "true",
                    "favoredContact": "true",
                    "main": "true",
                    "phone": "true",
                    "web": "false",
                    "mail": "false",
                    "highwayNum": "false",
                    "additionalNum": "false",
                    "highwayType": "false",
                    "highwayLabel": "false",
                    "addressLine2": "false",
                    "addressLine3": "false",
                    "addressLine5": "false",
                    "postalCode": "false",
                    "city": "false",
                    "country": "false",
                    "territory": "false",
                    "colorCode": "6750003",
                    "moduleCode": "CRM",
                    "orderNumber": "0"
                },
                "phone": row['x_num_tel']
            },
            "mainStore": row['id'],
            "customerAddresses": {
                "custId": row['x_num_tel'],
                "contactMethodTypeCode": "COU",
                "description": "Adresse client",
                "main": "1",
                "address": {
                    "addressId": row['x_ville'],
                    "addressLine1": row['x_ville'],
                    "addressLine2": row['x_ville'],
                    "addressLine3": row['x_ville'],
                    "addressLine4": "1",
                    "postalCode": row['CodePostal'],
                    "country": "TN",
                    "addressId2": row['AdresseID']
                }
            },
            "mainSupportCalculated": "true",
            "mysteryCustomer": "false",
            "state": "R"
        }
        
        for k, v in elements.items():
            if isinstance(v, dict):
                parent = ET.SubElement(values, k)
                for sub_k, sub_v in v.items():
                    if isinstance(sub_v, dict):
                        sub_parent = ET.SubElement(parent, sub_k)
                        for sub_sub_k, sub_sub_v in sub_v.items():
                            sub_sub_elem = ET.SubElement(sub_parent, sub_sub_k)
                            sub_sub_elem.text = str(sub_sub_v)
                    else:
                        sub_elem = ET.SubElement(parent, sub_k)
                        sub_elem.text = str(sub_v)
            else:
                elem = ET.SubElement(values, k)
                elem.text = str(v)
        
        for code, boolean_data in [('1', '0'), ('2', '1'), ('3', '0'), ('4', '0'), ('5', '0'), ('6', '0')]:
            datas = ET.SubElement(values, "datas")
            code_elem = ET.SubElement(datas, "code")
            code_elem.text = code
            data = ET.SubElement(datas, "data")
            booleanData = ET.SubElement(data, "booleanData")
            booleanData.text = boolean_data

    tree = ET.ElementTree(root)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)

def main():
    try:
        with psycopg2.connect(**db_params) as connection:
            df = pd.read_sql_query(query, connection)
            df = process_data(df)
            output_path = r"C:\Users\dwrjobs\Desktop\youssef\output.xml"
            create_xml(df, output_path)
            print("Fichier XML généré avec succès à l'emplacement spécifié.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erreur lors de la connexion à la base de données : {error}")

main()
