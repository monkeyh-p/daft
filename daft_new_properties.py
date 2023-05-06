import requests
from bs4 import BeautifulSoup
import json
import pymysql
import smtplib
from email.mime.text import MIMEText
import datetime
import re

def get_property_id_and_link(soup):
    ul_results = soup.find('ul', {'data-testid': 'results'})
    properties = []

    for li in ul_results.find_all('li', {'data-testid': re.compile(r'result-')}):
        property_id = li['data-testid'].replace('result-', '')
        properties.append((property_id, f'https://www.daft.ie/new-homes-for-sale/ireland?sort=publishDateDesc&search_id={property_id}'))
    
    return properties


def get_property_data(property_id):
    response = requests.get(f'https://www.daft.ie/_next/data/qkl785ecyPg1Va_7Tbsdx/property.json?id={property_id}')
    data = json.loads(response.text)

    try:
        property_info = data['pageProps']['listing']
    except KeyError:
        return None

    new_home = property_info.get('newHome', {})
    total_unit_types = new_home.get('totalUnitTypes', 0)

    unit_types = [
        {
            f"unittype_{i + 1}_price": int(re.sub('[a-zA-Z:]', '', sub_unit['price'].replace('€', '').replace(',', '').replace('POA', '0').replace('Price on Application', '0').strip())),
            f"unittype_{i + 1}_numBedrooms": sub_unit['numBedrooms'].split()[0] if sub_unit['numBedrooms'] else None,
            f"unittype_{i + 1}_propertyType": sub_unit['propertyType']
        }
        for i, sub_unit in enumerate(new_home.get('subUnits', [])[:4])  # Limit to only the first 4 unit types
    ]

    property_data = {
        'id': property_id,
        'title': property_info['title'],
        'price': int(re.sub('[a-zA-Z:]', '', property_info['price'].replace('€', '').replace(',', '').replace('POA', '0').replace('Price on Application', '0').strip())),
        'numBedrooms': property_info.get('numBedrooms', '').split()[0] if property_info.get('numBedrooms') else None,
        'propertyType': property_info['propertyType'],
        'lastUpdateDate': datetime.datetime.strptime(property_info['lastUpdateDate'], '%d/%m/%Y'),
        'date_inserted': datetime.datetime.now(),
    }

    for unit_type in unit_types:
        property_data.update(unit_type)

    return property_data

def insert_to_mysql(property_data, conn):
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM daft_new_properties WHERE id = %s", (property_data['id'],))
    count = cursor.fetchone()[0]

    if count == 0:
        keys = ', '.join(property_data.keys())
        values = ', '.join(['%s'] * len(property_data))
        query = f"INSERT INTO daft_new_properties ({keys}) VALUES ({values})"
        cursor.execute(query, tuple(property_data.values()))
        conn.commit()
        return True

    return False

def send_email(new_properties):
    body = f'Total rows scraped: {len(new_properties)}\nNew properties found:\n'
    for prop in new_properties:
        body += f"Link: {prop['link']}\nPrice: {prop['price']}\nTitle: {prop['title']}\n\n"

    msg = MIMEText(body)
    msg['Subject'] = 'New Properties Found'
    msg['From'] = 'monkeyhp@gmail.com'
    msg['To'] = 'monkeyhp@gmail.com'

    try:
        smtpObj = smtplib.SMTP('smtp.gmail.com', 587)
        smtpObj.starttls()
        smtpObj.login('monkeyhp@gmail.com', 'dsflrbxlibbzujmi')
        smtpObj.sendmail('monkeyhp@gmail.com', 'monkeyhp@gmail.com', msg.as_string())
        smtpObj.quit()
    except Exception as e:
        print(f"Error: {e}")


def main():
    base_url = 'https://www.daft.ie/new-homes-for-sale/ireland?sort=publishDateDesc&pageSize=20&from='
    max_pages = 20
    page_size = 20
    new_properties = []

    conn = pymysql.connect(
        host='192.168.86.198',
        user='chris',
        password='Xb0x3483',
        database='amazon_product'
    )

    for page in range(max_pages):
        print(f"Scraping page {page + 1} of {max_pages}")
        response = requests.get(base_url + str(page * page_size))
        soup = BeautifulSoup(response.text, 'html.parser')

        properties = get_property_id_and_link(soup)
        property_ids = [prop_id for prop_id, _ in properties]

        for property_id in property_ids:
            property_data = get_property_data(property_id)
            if property_data is None:
                continue

            is_new = insert_to_mysql(property_data, conn)
            if is_new:
                new_property = {
                    'link': f'https://www.daft.ie/new-homes-for-sale/ireland?sort=publishDateDesc&search_id={property_data["id"]}',
                    'price': property_data['price'],
                    'title': property_data['title']
                }
                new_properties.append(new_property)

                # Print the title, price, and property ID to the console
                print(f"New property found: ID={property_data['id']}, Title={property_data['title']}, Price={property_data['price']}")

    send_email(new_properties)
    conn.close()

if __name__ == '__main__':
    main()
