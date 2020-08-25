"""
Data Engineer trial project
By NMunoz

To summarize, below is an almost workable pseudocode written in python syntax. 
The psuedocode below scrapes the website and for each identifier requested
stores the desired variables into beautifulsoup objects. I use comments to explain
how given more time, I would add the following:

1. logging instead of printing
2. Functions instead of just for loops
3. Imputation functions to ensure data integrity and congruity
4. DataFrames for Machine Learning Researchers to have multiple data storing options
5. Testing units/metrics to ensure that each section works

I wrote in psuedocode because I'm working from a Chromebook. 

"""

import pandas 
import requests
import pprint
import psycopg2
from bs4 import BeautifulSoup


url1='https://www.drugbank.ca/drugs/'
url2=['DB00619', 'DB01048', 'DB14093', 'DB00173', 'DB00734', 'DB00218', 'DB05196',
'DB09095', 'DB01053', 'DB00274']

for i in range(url2): 
    url=url1+url2[i]
    print(url)
    
    wpage = requests.get(url)
    pprint.pprint(wpage.content)

    #here I would use .log instead to start logging some of the data I'm scraping for easier troubleshooting later

    ph_wb_elems = wpage.find_all('main', class_='content-container')

    #The for loop will go through all of the elements in the beautiful soup object and save the Smiles info, the gene name
    #and the actions
    for ph_wb_elem in ph_wb_elems:
     # Each ph_wb_elems is a new BeautifulSoup object.
        rx_id=ph_wb_elems.find('dd', class_='col-xl-4 col-md-9 col-sm-8')
        smiles_elem = ph_wb_elems.find(id='smiles')
        gene_nm = ph_wb_elems.find('dd', class_='col-md-7 col-sm-6')
        actions_elem = ph_wb_elems.find('div', class_='badge badge-pill badge-action')
        print(rx_id)
        print(smiles_elem)
        print(gene_nm)
        print(actions_elem)
#Again, we should log instead of print, but in the interest of time I will print

    
#Now I'll save all of the objects into a table in postgres with the try catch below
#I might need more time to think this through in order to properly imputate the values saved above
#I might want to write some functions here to clean the data and ensure that the datatypes will match the datatypes in my
#PostgresSQL database
#I might also want to save the above for loop into a function and finally return a dataframe so that we have the option of choosing 
#to visualize our data using python dataframes rather than going through SQL.

try:
   connection = psycopg2.connect(user="sysadmin",
                                  password="yrpassword",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres_db")
   cursor = connection.cursor()

   postgres_insert_query = """ INSERT INTO pharma_tbl_src_x (RX_ID, SMILES, GENE_NM, ACTIONS_ELEM) VALUES (%s,%s,%s,%s)"""
   record_to_insert = (rx_id, smiles_elem, gene_nm, actions_elem)
   cursor.execute(postgres_insert_query, record_to_insert)

   connection.commit()
   count = cursor.rowcount
   print (count, "Record inserted successfully into pharma_tbl_src_x table")

except (Exception, psycopg2.Error) as error :
    if(connection):
        print("Failed to insert record into pharma_tbl_src_x table", error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
