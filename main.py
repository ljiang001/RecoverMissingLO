from configparser import ConfigParser
from multiprocessing import Process, Queue
import argparse
import urllib.parse
import psycopg2
import time
from datetime import datetime
import sys
import os

parser = argparse.ArgumentParser()

parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    default="config.ini",
)
parser.add_argument(
    "-n",
    "--number-thread",
    help="number of parallel threads, default: 10",
    type=int,
    default=10,
)
args = parser.parse_args()

def build_config(config_file):
    config = {}
    parser = ConfigParser()
    parser.read(config_file)
    for section in parser.sections():
        config[section] = {}
        params = parser.items(section)
        for param in params:
            config[section][param[0]] = param[1]
    return config

CONFIG = build_config(args.config_file)
SOURCE_CONFIG = CONFIG["source"]
TARGET_CONFIG = CONFIG["target"]


def verify_db_connections(config):
    for section in ["source", "target"]:
        print(f"Verifying connection to {section} db ...")
        db_conn = build_db_connection(CONFIG[section])
        if not db_conn:
            logging.info(f"Failed to connect to {section} db.")
            return False
    return True

def build_db_connection(db_config):
    conn_string = build_connection_string(db_config)
    db_conn = psycopg2.connect(conn_string)
    return db_conn

def build_connection_string(db_config):
    conn_string = f'postgres://{urllib.parse.quote(db_config["user"])}:{urllib.parse.quote(db_config["password"])}'
    conn_string += f'@{urllib.parse.quote(db_config["host"])}:{urllib.parse.quote(db_config["port"])}'
    conn_string += f'/{urllib.parse.quote(db_config["database"])}'
    if db_config["sslmode"].strip():
        conn_string += f'?sslmode={urllib.parse.quote(db_config["sslmode"])}'
    return conn_string

def lo_recover_by_range(min_oid, max_oid):
    print(f"recoving oid from {min_oid} to {max_oid} ...")
    log_success_file = open("log_success", "a+")
    log_failure_file = open("log_failure", "a+")

    source_conn = build_db_connection(SOURCE_CONFIG)
    target_conn = build_db_connection(TARGET_CONFIG)
    source_conn_str = build_connection_string(SOURCE_CONFIG)
    source_cur = source_conn.cursor()
    target_cur = target_conn.cursor()

    # find oid that exist in source but not in target
    select_oid_query = f"SELECT loid FROM pg_largeobject WHERE loid >={min_oid} AND loid <= {max_oid};"
    target_cur.execute(select_oid_query)
    target_oids_set = set()
    target_oid_row = target_cur.fetchone()
    while target_oid_row:
        target_oids_set.add(target_oid_row[0])
        target_oid_row = target_cur.fetchone()
    to_recover_oid_set = set()
    source_cur.execute(select_oid_query)
    source_oid_row = source_cur.fetchone()
    while source_oid_row:
        if source_oid_row[0] not in target_oids_set:
            to_recover_oid_set.add(source_oid_row[0])
        source_oid_row = source_cur.fetchone()
    if(len(to_recover_oid_set) == 0):
        print(f"No missing oid from {min_oid} to {max_oid}")
        return
    else:
        print(f"{len(to_recover_oid_set)} missing oid from {min_oid} to {max_oid}")
    for oid in to_recover_oid_set:
        filename = f"lo_{oid}"
        command = f'psql "{source_conn_str}" -c "\lo_export {oid} {filename}"'
        try:
            os.system(command)
            fd = open(filename, "r")
            target_cur.execute(f"SELECT lo_create({oid});")
            lobj = target_conn.lobject(oid, 'w', oid)
            lobj.write(fd.read())
            target_conn.commit()
            os.system(f"rm {filename}")
            print(f"Successfully recovered for oid: {oid}")
            log_success_file.write(f"{oid}\n")
        except Exception as err:
            print(f"Failed to recover for oid: {oid}, error:")
            print(err)
            log_failure_file.write(f"{oid}\n")
            target_conn = build_db_connection(TARGET_CONFIG)
            target_cur = target_conn.cursor()

def start_process(procs):
    for p in procs:
        p.start()
    for p in procs:
        p.join()

def main(min_oid, max_oid):
    if not verify_db_connections(CONFIG):
        quit()

    task_queue = Queue()
    step = 1000
    count = 0
    for oid in range(min_oid, max_oid+1, step):
        task_queue.put(f"{oid},{oid + step}")
        count += 1
    
    def execute_tasks(thread):
        print(f"start thread: {thread}")
        while not task_queue.empty():
            oids = task_queue.get().split(",")
            lower_oid, upper_oid = int(oids[0]), int(oids[1])
            lo_recover_by_range(lower_oid, upper_oid)
    
    recover_procs = [
        Process(target=execute_tasks, args=(i,))
        for i in range(min(args.number_thread, count))
    ]

    start_process(recover_procs)

if __name__ == "__main__":
    MIN_OID = 90000
    MAX_OID = 100000
    main(MIN_OID, MAX_OID)