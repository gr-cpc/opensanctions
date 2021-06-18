import psycopg2
import psycopg2.extras
from elastic_enterprise_search import AppSearch
import json
import pprint
from pathlib import Path
import time
import settings

POSTGRE_PARAMS = settings.POSTGRE_PARAMS
AS_ADDRESS = settings.AS_ADDRESS
AS_HTTP_AUTH = settings.AS_HTTP_AUTH
AS_ENGINE = settings.AS_ENGINE
CHECK_PATH = settings.CHECK_PATH


'''
def connect():
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(POSTGRE_PARAMS)
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


def wait_for_crawl_check(path, sleep_seconds=60, max_attempts=30):
    print('Checking crawl job status.')
    i = 0
    while i<max_attempts:
        try:
            with open(path, 'r') as f:
                j = json.loads(json.load(f))
                if j['crawl_complete'] == False:
                    print('...crawl job not complete yet, waiting {} seconds.'.format(sleep_seconds))
                    time.sleep(sleep_seconds)
                    i+=1
                else:
                    print('Crawl job complete! Starting extraction.')
                    break
                    
        except:
            print('...crawl status file not available, waiting {} seconds.'.format(sleep_seconds))
            time.sleep(sleep_seconds)
            i+=1


def set_check_to_false(path):
    print('Resetting crawl status.')
    data = json.dumps({'crawl_complete':False}, indent=4)
    try:
        f = open(path, 'w')
        json.dump(data, f)
    except:
        f = open(path, 'x')
        json.dump(data, f)
    finally:
        f.close()


def fetch_rows_from_postgre():
    conn= None
    f = None
    try:
        print('Connecting to PostgreSQL database...')
        conn = psycopg2.connect(POSTGRE_PARAMS)
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        #cur.execute("select distinct(schema), string_agg(distinct(prop), '; ') from statement group by schema")
        cur.execute('''
            CREATE EXTENSION IF NOT EXISTS tablefunc;
            SET TIMEZONE='Asia/Hong_Kong';
            SELECT *
            FROM crosstab(
                $$SELECT entity_id, dataset, schema, to_char(first_seen::TIMESTAMP WITHOUT TIME ZONE, 'FXYYYY-MM-DD"T"HH:MI:SS"Z"'), 
                         to_char(last_seen::TIMESTAMP WITHOUT TIME ZONE, 'FXYYYY-MM-DD"T"HH:MI:SS"Z"'), prop, value
                    FROM (
                        SELECT entity_id, dataset, schema, min(first_seen) AT TIME ZONE 'UTC' as first_seen, max(last_seen) AT TIME ZONE 'UTC' as last_seen, prop,  string_agg(value, '; ') as value 
                        FROM statement
                        GROUP BY entity_id, dataset, schema, prop, first_seen, last_seen
                    ) AS subquery
                    WHERE schema IN ('Person', 'LegalEntity', 'Company', 'Organization', 'Thing', 'PublicBody') 
                    AND last_seen = ( SELECT max(last_seen) AT TIME ZONE 'UTC' from statement )
                    ORDER BY 1,2$$,
                $$VALUES ('name'), ('firstName'), ('secondName'), ('middleName'), ('lastName'), ('alias'), ('previousName'), ('nationality'), ('country'::text),
                ('position'), ('legalForm'), ('description'), ('notes'), ('modifiedAt')$$
            ) AS ct (id varchar, dataset varchar, schema text, first_seen text, last_seen text, name text, first_name text, second_name text, 
                middle_name text, last_name text, alias text, previous_name text,  nationality text, country text, position text, legal_Form text, 
                description text, notes text, modified_at text);
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


def connect_to_AS():
    print('Connecting to AppSearch host: {}'.format(AS_ADDRESS))
    app_search = AppSearch(AS_ADDRESS, http_auth=AS_HTTP_AUTH)
    return app_search

def index_rows_to_AS(app_search, f, interval=100):
    assert f!=None
    try:
        errors = 0
        for i in range(0, len(f), interval):
            print(' - Uploading Records {} to {}'.format(i, i+interval-1))
            j = json.dumps(f[i:i+interval])
            
            resp = app_search.index_documents(
                engine_name=AS_ENGINE,
                documents=j
            )
            for d in resp:
                if len(d['errors'])>0:
                    print('resp errors: {}'.format(d['errors']))
                    errors += 1
        print("Indexing completed. Errors raised: {}.".format(errors))
    except Exception as error:
        print(error)


def check_create_engine(app_search):
    print("Looking for engine '{}': ".format(AS_ENGINE), end="")
    app_search = AppSearch(AS_ADDRESS, http_auth=AS_HTTP_AUTH)
    try:
        _ = app_search.get_engine(engine_name=AS_ENGINE)
        print('FOUND.')
    except:
        print("NOT FOUND.\nCreating new engine '{}'.".format(AS_ENGINE))
        r = app_search.create_engine(
            engine_name=AS_ENGINE,
            language=None
        )


def main():
    try:
        # Wait for crawl jobs to finish checks
        wait_for_crawl_check(CHECK_PATH)
        AS_client = None
        cta_attempt = 0
        
        # Connection to AppSearch attempts
        while cta_attempt < 5:
            try:
                AS_client = connect_to_AS()
                check_create_engine(AS_client)
                break
            except:
                cta_attempt += 1
                print("Attempt {}: connect_to_AS failed.".format(cta_attempt))
        
        # Retrieval from PostgreSQL & indexing to AppSearch attempts
        index_attempt = 0
        while index_attempt < 5:
            try:
                index_rows_to_AS(AS_client, fetch_rows_from_postgre())
                break
            except:
                index_attempt += 1
                print("Attempt {}: fetching or indexing rows failed.".format(index_attempt))
    finally:
        set_check_to_false(CHECK_PATH)


if __name__=="__main__":
    main()
