#!/usr/bin/python3

import requests
import psycopg2
import multiprocessing
import sys
import os
import subprocess
import math

__author__ = 'Lev Kokotov <lev.kokotov@instacart.com>'
__version__ = 0.1

DEFAULT_PG_DUMP_BACKUP_DIR = os.path.join(os.environ.get('HOME'), 'Desktop')
DEFAULT_POSTGRES_DB = 'rollbars'
DEFAULT_POSTGRES_HOST = '127.0.0.1'
DEBUG = os.environ.get('DEBUG', False)

def __execute(cmd):
    '''Execute a command on the system.

    Arguments:
        - cmd: str, command to execute in the shell
    '''
    if DEBUG:
        print(f'Executing: {cmd}')

    return subprocess.check_output(cmd)


def __unix_user():
    '''Get the current unix user.'''
    return __execute('whoami').replace(b"\n", b'').decode('utf-8')


def __dbname():
    '''Get desired database name.'''
    return os.environ.get('POSTGRES_DB', DEFAULT_POSTGRES_DB)


def setup_rollbar_id(counter: int):
    '''Get the global Rollbar ID for a counter.
    
    Arguments:
        - counter: int
    '''
    rollbar_token = os.environ.get('ROLLBAR_TOKEN')
    resp = requests.get(f'https://api.rollbar.com/api/1/item_by_counter/{counter}?access_token={rollbar_token}')

    try:
        id_ = resp.json()['result']['id']
        os.environ['ROLLBAR_ID'] = str(id_) # Accessible from all subprocesses
    except:
        print(f'Could not find Rollbar ID with counter {counter} and given project access token.')
        print('Perhaps the acess token is for the wrong project? Counters are per project, not global.')
        exit(1)


def setup_db(backup=False):
    '''Setup the database from scratch.
    
    Arguments:
        - cursor: psycopg2 cursor
        - conn: psycopg2 connection
        - db_name: str, name of the database
        - backup: boolean, backup or not to backup existing DB
    '''
    cursor, conn = psql()
    dbname = __dbname()

    # Doesn't work yet :/
    if backup:
        backup_dir = os.environ.get('PG_DUMP_BACKUP_DIR', DEFAULT_PG_DUMP_BACKUP_DIR)
        __execute(f'pg_dump {dbname} -f {backup_dir}/{dbname}.sql')

    cursor.execute('''
        DROP TABLE IF EXISTS rollbars;
    ''')

    cursor.execute('''
        CREATE TABLE rollbars (
            id BIGINT NOT NULL UNIQUE, -- Rollbar instance ID is unique
            project_id BIGINT,
            environment VARCHAR,
            request_path VARCHAR,
            status_code VARCHAR,
            "timestamp" INT,
            level VARCHAR
        );
    ''')

    conn.commit()
    conn.close()


def insert(rollbar, cursor):
    '''Insert a Rollbar into the DB.
    
    Arguments:
        - rollbar: dict, rollbar info
        - cursor: psycopg2 cursor
    '''
    id_ = rollbar['id']
    project_id = rollbar['project_id']
    timestamp = rollbar['timestamp']
    environment = rollbar['data']['environment']
    level = rollbar['data']['level']
    status_code = rollbar['data']['body']['message']['extra']['status_code']
    request_path = rollbar['data']['body']['message']['extra']['request_path']
    try:
        error_message = rollbar['data']['body']['message']['extra']['error_message']
    except:
        error_message = ''

    try:
        # I trust inputs from Rollbar so I won't escape them.
        cursor.execute(f"""
            INSERT INTO rollbars
            (id, project_id, environment, request_path, status_code, "timestamp", level)
            VALUES (
                {id_}, {project_id}, '{environment}', '{request_path}',
                '{status_code}', {timestamp}, '{level}'
            );
        """)
    except psycopg2.errors.UniqueViolation:
        # Skip duplicates
        pass


def get(page: int):
    '''Get the occurences of the Rollbar.
    
    Arguments:
        - page: int, the rollbars are paginated (like almost any API)
    '''
    rollbar_id = os.environ.get('ROLLBAR_ID')
    rollbar_token = os.environ.get('ROLLBAR_TOKEN')
    resp = requests.get(f'https://api.rollbar.com/api/1/item/{rollbar_id}/instances?access_token={rollbar_token}&page={page}')

    # Create Postgres connection
    cursor, conn = psql()
    
    try:
        for rollbar in resp.json()['result']['instances']:
            insert(rollbar, cursor)
        conn.commit()
    except Exception as e:
        print(f'Failed to fetch Rollbars: {e}')
        print(f'Page {page}')
        conn.rollback()
    finally:
        conn.close()

    if DEBUG:
        print(f'Done with page {page}')


def psql():
    '''Get cursor and connection to Postgres.'''
    # Brew sets up Postgres with the system user
    dbname = __dbname()
    user = os.environ.get('POSTGRES_USER', __unix_user())
    host = os.environ.get('POSTGRES_HOST', DEFAULT_POSTGRES_HOST)

    conn_string = f'host={host} dbname={dbname} user={user}'

    if DEBUG:
        print(f'Connecting to {conn_string}')

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    return cursor, conn


def main(counter, num_rollbars):
    '''Entrypoint.

    Arguments:
        - counter: Rollbar counter (i.e. http://rollbar.com/company/project/<counter>)
        - num_rollbars: How many rollbars to fetch (from the beginning of time)
    '''

    setup_db(backup=False)
    setup_rollbar_id(counter)

    with multiprocessing.Pool(20) as pool:
        pages = math.ceil(num_rollbars / 20) # Rollbar returns pages of 20 items.
        pool.map(get, range(1, pages + 1)) # Page count starts at 1


# Go!
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage:\n\nROLLBAR_TOKEN=<token> ./scrape_rollbar.py <counter> <number of rollbars to fetch>\n')
        exit(1)

    counter = sys.argv[1]
    num_rollbars = int(sys.argv[2])

    main(counter, num_rollbars)
