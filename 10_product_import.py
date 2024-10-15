import pandas
from neo4j import GraphDatabase
from neo4j_utils.csv_import import import_csv
import os

from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
DB_NAME = os.getenv("NEO4J_DATABASE")

driver = GraphDatabase.driver(URI, auth=AUTH, database=DB_NAME)
driver.verify_connectivity()

driver.execute_query("CREATE DATABASE $database IF NOT EXISTS WAIT", database_="system", database=DB_NAME)
driver.execute_query("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Category) REQUIRE n.id IS UNIQUE", database_=DB_NAME)
driver.execute_query("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Product) REQUIRE n.id IS UNIQUE", database_=DB_NAME)

import_csv(driver,
           """
           SELECT categoryID as id, categoryName, description 
           FROM read_csv('data/categories.csv')
           """,
           """
           UNWIND $rows as row
           MERGE (cat:Category {id:row.id}) SET cat += row
           """)


# The discontinued info in the source data is stored as 0/1. Transform this into booleans.
def discontinued_transform(df: pandas.DataFrame) -> pandas.DataFrame:
    df['discontinued'] = df['discontinued'].apply(lambda x: True if x == 1 else False)
    return df


import_csv(driver,
           """
           SELECT productID as id, productName, categoryID, quantityPerUnit, unitPrice, discontinued
           FROM read_csv('data/products.csv')
           """,
           """
           UNWIND $rows as row
           MATCH (cat:Category {id: row.categoryID})
           // WITH cat
           MERGE (p:Product {id:row.id}) SET p += row
           MERGE (p)-[:HAS_CATEGORY]->(cat)
           """, discontinued_transform)

# cleanup useless/redundant properties
driver.execute_query("MATCH (p:Product) SET p.categoryID = null")

driver.close()
