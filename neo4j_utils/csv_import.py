import time

import duckdb
from neo4j import Driver

conn = duckdb.connect()


def send_chunk_to_neo4j(tx, cypher_query, chunk):
    tx.run(cypher_query, rows=chunk.to_dict('records')).consume()
    print('.', end='', flush=True)


def import_csv(driver: Driver, sql_query: str, cypher_query: str, vectors_size: int = 5):
    """
    :param driver: the neo4j driver
    :param sql_query: duckdb query used to read data from the csv file
    :param cypher_query: the cypher used to insert data. data is exposed under the $rows parameter
    :param vectors_size: The duckdb vectors_per_chunk
    """
    start_time = time.time()
    rs = conn.execute(f"{sql_query}")
    while True:
        chunk = rs.fetch_df_chunk(vectors_size)  # nb rows == chunk_size * 2048 which is duckdb vector size
        if chunk.empty:
            break
        with driver.session() as session:
            session.execute_write(send_chunk_to_neo4j, cypher_query, chunk)
    print("\nDone in ", time.time() - start_time, " sec")
