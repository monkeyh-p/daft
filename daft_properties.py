import requests
from bs4 import BeautifulSoup
import json
import pymysql
import smtplib
from email.mime.text import MIMEText
import datetime
import re
import concurrent.futures

def get_property_id_and_link(soup):
    # Find the container element (ul) that holds the property listings using the specified data-testid attribute
    property_listings_container = soup.find('ul', {'data-testid': 'results'})
    # Find all property listing elements (li) with the 'data-testid' attribute
    property_listing_elements = property_listings_container.find_all('li', {'data-testid': True})
    properties = []

    for prop in property_listing_elements:
        # Extract the property id from the 'data-testid' attribute
        property_id = prop['data-testid'].replace('result-', '')

        properties.append((property_id, f'https://www.daft.ie/property-for-sale/ireland?sort=publishDateDesc&search_id={property_id}'))
    return properties



def get_property_data(property_id):
    response = requests.get(f'https://www.daft.ie/_next/data/qkl785ecyPg1Va_7Tbsdx/property.json?id={property_id}')
    
    try:
        data = json.loads(response.text)
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON for property ID: {property_id}")
        return None
    
    try:
        property_info = data['pageProps']['listing']
    except KeyError:
        return None
    
    # Get 'numBedrooms' value if the key exists, otherwise set it to an empty string
    price = property_info.get('price', '')
    numBedrooms = property_info.get('numBedrooms', '')
    
    last_update_date = datetime.datetime.strptime(property_info['lastUpdateDate'], '%d/%m/%Y')
    return {
        'id': property_id,
        'title': property_info['title'],
        'price': int(''.join(re.findall('\d+', property_info['price'].replace('€', '').replace(',', '').replace('POA', '0').replace('Price on Application', '0').strip()))),
        'numBedrooms': numBedrooms,
        'lastUpdateDate': last_update_date,
    }

def insert_to_mysql(property_data, conn):
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM daft_properties WHERE id = %s", (property_data['id'],))
    count = cursor.fetchone()[0]

    if count == 0:
        cursor.execute("""
            INSERT INTO daft_properties (id, title, price, numBedrooms, lastUpdateDate, date_inserted)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            property_data['id'],
            property_data['title'],
            property_data['price'],
            property_data['numBedrooms'],
            property_data['lastUpdateDate'],
            datetime.datetime.now(),
        ))
        conn.commit()
        return True

    cursor.execute("SELECT price FROM daft_properties WHERE id = %s ORDER BY date_inserted DESC LIMIT 1", (property_data['id'],))
    latest_price = cursor.fetchone()[0]

    if latest_price != property_data['price']:
        cursor.execute("""
            INSERT INTO daft_properties (id, title, price, numBedrooms, lastUpdateDate, date_inserted)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            f'{property_data["id"]}-{count}',
            property_data['title'],
            property_data['price'],
            property_data['numBedrooms'],
            property_data['lastUpdateDate'],
            datetime.datetime.now(),
        ))
        conn.commit()
        return True

    return False

def send_email(new_properties):
    body = f'Total rows scraped: {len(new_properties)}\nNew properties found:\n'
    for prop in new_properties:
        body += f"Link: {prop['link']}\nPrice: {prop['price']}\nTitle: {prop['title']}\n\n"

    msg = MIMEText(body)
    msg['Subject'] = 'New Properties on Daft.ie'
    msg['From'] = 'monkeyhp@gmail.com'
    msg['To'] = 'monkeyhp@gmail.com'

    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login('monkeyhp@gmail.com', 'dsflrbxlibbzujmi')
    server.sendmail('monkeyhp@gmail.com', ['monkeyhp@gmail.com'], msg.as_string())
    server.quit


def get_property_data_with_retries(property_id, max_retries=3):
    for _ in range(max_retries):
        property_data = get_property_data(property_id)
        if property_data is not None:
            return property_data
    return None


def main():
    base_url = 'https://www.daft.ie/property-for-sale/ireland?sort=publishDateDesc&pageSize=20&from='
    max_pages = 820
    page_size = 20
    new_properties = []

    conn = pymysql.connect(
        host='192.168.86.198',
        user='chris',
        password='Xb0x3483',
        database='amazon_product'
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for page in range(max_pages):
            page_start = page * page_size
            url = f'{base_url}{page_start}'
            response = requests.get(url)

            if response.status_code != 200:
                print(f'Error {response.status_code} at page {page}')
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            properties = get_property_id_and_link(soup)

            print(f'Processing page {page + 1} of {max_pages}')  # Display the current page number

            # Fetch property data in parallel
            property_data_list = list(executor.map(get_property_data_with_retries, [prop_id for prop_id, _ in properties]))

            for (prop_id, prop_link), property_data in zip(properties, property_data_list):
                if property_data is None:
                    continue

                # Display some of the property content being scraped
                print(f"Scraping property ID: {property_data['id']}, Title: {property_data['title']}, Price: {property_data['price']}")

                is_new = insert_to_mysql(property_data, conn)
                if is_new:
                    new_properties.append({
                        'link': prop_link,
                        'price': property_data['price'],
                        'title': property_data['title']
                    })

    conn.close()

    if new_properties:
        send_email(new_properties)

if __name__ == '__main__':
    main()

