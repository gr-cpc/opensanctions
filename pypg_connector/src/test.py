import psycopg2
import psycopg2.extras
from elastic_enterprise_search import AppSearch
import json
import pprint
from pathlib import Path
import time

PARAMS = "host=opensanctions_postgres_1 dbname=opensanctions user=opensanctions password=opensanctions"
AS_ADDRESS= "http://10.180.32.84:3002"
AS_HTTP_AUTH="private-hk795xu37hb8seqqn9bitmon"
AS_ENGINE="opensanctions"
CHECK_PATH = '../data/check/status.json'

'''
def connect():
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(PARAMS)
        cur = conn.cursor()
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
          
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
'''


def wait_for_crawl_check(path, sleep_seconds=60):
    print('Checking crawl job status.')
    while True:
        try:
            with open(path, 'r') as f:
                j = json.loads(json.load(f))
                if j['crawl_complete'] == False:
                    print('...crawl job not complete yet.')
                else:
                    print('Crawl job complete! Starting extraction.')
                    break
                    
        except:
            print('...could not read crawl status file.')
        finally:
            time.sleep(sleep_seconds)


def set_check_to_false(path):
    data = json.dumps({'crawl_complete':False}, indent=4)
    try:
        f = open(path, 'w')
        json.dump(data, f)
    except:
        f = open('check.json', 'x')
        json.dump(data, f)
    finally:
        f.close()


def fetch_rows():
    conn= None
    f = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(PARAMS)
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        #cur.execute("select distinct(schema), string_agg(distinct(prop), '; ') from statement group by schema")
        cur.execute('''
            CREATE EXTENSION IF NOT EXISTS tablefunc;
            select *
            from crosstab(
                $$SELECT entity_id, dataset, schema, prop, value
                     FROM (
                        select entity_id, dataset, schema, prop, string_agg(value, '; ') as value from statement
                        group by entity_id, dataset, schema, prop 
                        ) AS subquery where schema in ('Person', 'LegalEntity', 'Company', 'Organization', 'Thing', 'PublicBody') 
                        ORDER BY 1,2$$
                , $$VALUES ('name'), ('firstName'), ('secondName'), ('middleName'), ('lastName'), ('alias'), ('previousName'), ('nationality'), ('country'::text),
                ('position'), ('legalForm'), ('description'), ('notes'), ('modifiedAt')$$
                ) AS ct (id varchar, dataset varchar, schema text, name text, first_name text, second_name text, middle_name text, last_name text, alias text,
                        previous_name text,  nationality text, country text, position text, legal_Form text, description text, notes text,
                        modified_at text)
        ''')
        
        print("Number of rows: ", cur.rowcount)
        
        f = cur.fetchall()
        
        cur.close()
        
        return f
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        return f


def index_rows(f, interval=100):
    assert f!=None
    try:
        print('Connecting to AppSearch host: {}'.format(AS_ADDRESS))
        client_side = AppSearch(AS_ADDRESS, http_auth=AS_HTTP_AUTH)
        errors = 0
        for i in range(0, len(f), interval):
            print(' - Uploading Records {} to {}'.format(i, i+interval-1))
            j = json.dumps(f[i:i+interval])
            
            resp = client_side.index_documents(
                engine_name=AS_ENGINE,
                documents=j
            )
            for d in resp:
                if len(d['errors'])>0:
                    print(d['errors'])
                    errors += 1
        print("Indexing completed. Errors raised: {}.".format(errors))
    except Exception as error:
        print(error)


def main():
    wait_for_crawl_check(CHECK_PATH)
    index_rows(fetch_rows())
    set_check_to_false(CHECK_PATH)


if __name__=="__main__":
    main()
