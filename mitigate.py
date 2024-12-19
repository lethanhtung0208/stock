import psycopg2
import os
import subprocess
import csv

postgres_config = {
    'dbname': 'stock',
    'user': 'stock',
    'password': 'stock',
    'host': 'localhost',
    'port': 5432
}

def dump_schema(source_dbname, source_user, source_host, output_file):
    try:
        dump_command = [
            r"C:\Program Files\PostgreSQL\17\bin\pg_dump",
            "-U", source_user,
            "-h", source_host,
            "-s",
            "-f", output_file,
            source_dbname
        ]
        subprocess.run(dump_command, check=True)
        print(f"Schema successfully dumped to {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while dumping schema: {e}")

def restore_schema(target_dbname, target_user, target_host, target_port, input_file):
    try:
        os.environ['PGPASSWORD'] = 'stock'
        if not os.path.exists(input_file):
            print(f"SQL file '{input_file}' does not exist.")
        psql_path = "C:\\Program Files\\PostgreSQL\\17\\bin\\psql.exe"
        if not os.path.exists(psql_path):
            print(f"PostgreSQL file '{psql_path}' does not exist.")
        restore_command = [
            psql_path,
            "-U", target_user,   # Target PostgreSQL user
            "-h", target_host,   # Target host
            "-p", str(target_port),  # Target port as a string
            "-d", target_dbname,  # Target database name
            "-f", input_file  # SQL file to be executed
        ]
        print("Command to run:", " ".join(restore_command))
        subprocess.run(restore_command, check=True)
        print(f"Schema successfully restored to {target_dbname}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while restoring schema: {e}")

sql_directory = r'C:\stock\db'

def connect_postgres():
    return psycopg2.connect(**postgres_config)

def run_postgres_query(query):
    conn = connect_postgres()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

def get_postgres_tables():
    conn = psycopg2.connect(**postgres_config)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE';
    """)
    tables = cursor.fetchall()
    cursor.close()
    conn.close()
    return [table[0] for table in tables]

def export_table_to_csv(table_name):
    conn = psycopg2.connect(**postgres_config)
    cursor = conn.cursor()
    csv_file_path = os.path.join(sql_directory, f"{table_name}.csv")

    cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """)
    columns = cursor.fetchall()

    column_list = []
    for col_name, data_type in columns:
        escaped_col_name = f'"{col_name}"'  
        if data_type in ['character varying', 'text', 'varchar']:
            column_list.append(f"COALESCE({escaped_col_name}, '') AS {escaped_col_name}")
        
        elif data_type in ['integer', 'bigint']:
            column_list.append(f"COALESCE({escaped_col_name}, NULL) AS {escaped_col_name}")
        elif data_type in ['numeric', 'real', 'double precision']:
            column_list.append(f"COALESCE(NULLIF({escaped_col_name}, 'NaN'), NULL) AS {escaped_col_name}")
        else:
            column_list.append(f"{escaped_col_name}")
    
    coalesce_columns = ", ".join(column_list)
    query = f"COPY (SELECT {coalesce_columns} FROM {table_name}) TO STDOUT WITH CSV HEADER DELIMITER ','"
    with open(csv_file_path, 'w', newline='') as f:
        cursor.copy_expert(query, f)
    
    print(f"Exported {table_name} to {csv_file_path}")
    cursor.close()
    conn.close()
    return csv_file_path

def delete_from_table(table_name):
    delete_sql = f"DELETE FROM {table_name};"
    
    conn = connect_postgres()
    cursor = conn.cursor()
    
    cursor.execute(delete_sql)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Deleted rows from {table_name}")

def import_csv(table_name):
    csv_file_path = os.path.join(sql_directory, f"{table_name}.csv")
    with open(csv_file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
    
    columns = ', '.join(header)
    copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV HEADER DELIMITER ','"
    conn = connect_postgres()
    cursor = conn.cursor()
    with open(csv_file_path, 'r') as f:
        cursor.copy_expert(sql=copy_sql, file=f)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Imported {table_name}")

def export_tables():
    tables = get_postgres_tables()
    tables = [table for table in tables if "_20240930" in table]
    # tables.append('user_config')
    tables = ['ticker_code_mapping']
    
    # dates = [
    #     (2024, 10, 11), (2024, 10, 16)
    # ]
    # for date in dates:
    #     date_str = f"{date[0]:04d}{date[1]:02d}{date[2]:02d}"
    #     export_table_to_csv(f"real_time_sum_interval_{date_str}")
    # tables = ["real_time_data_interval_20241001"]
    for table in tables:
        export_table_to_csv(table)

def delete_from_tables():
    tables = get_postgres_tables()
    tables.remove('ticker_code_mapping')
    tables.append('ticker_code_mapping')
    tables = ["ask_bid_data_partitioned"]
    for table in tables:
        delete_from_table(table)

def import_tables():
    tables = get_postgres_tables()
    # tables = [table for table in tables if "_partitioned" in table]
    # tables.append('user_config')
    # tables.append('ticker_code_mapping')
    # tables.remove('ticker_code_mapping')
    # tables.insert(0, 'ticker_code_mapping')
    # tables = ["real_time_data"]
    tables = ["ticker_code_mapping", "historical_data"]
    for table in tables:
        import_csv(table)
    dates = [
        (2024, 9, 26), (2024, 9, 27), (2024, 9, 30), (2024, 10, 1), (2024, 10, 2), (2024, 10, 3), (2024, 10, 4),
        (2024, 10, 7), (2024, 10, 8), (2024, 10, 9), (2024, 10, 10), (2024, 10, 11), (2024, 10, 15), (2024, 10, 16),
        (2024, 10, 29), (2024, 10, 30), (2024, 10, 31), (2024, 11, 1), (2024, 11, 5), (2024, 10, 17), (2024, 10, 18), 
        (2024, 10, 21), (2024, 10, 22), (2024, 10, 23), (2024, 10, 24), (2024, 10, 25), (2024, 10, 28),
        (2024, 11, 6), (2024, 11, 8), (2024, 11, 11), (2024, 11, 12), (2024, 11, 13), (2024, 11, 14), (2024, 11, 15)
    ]
    conn = connect_postgres()
    cursor = conn.cursor()
    for date in dates:
        date_str = f"{date[0]:04d}{date[1]:02d}{date[2]:02d}"
        drop_sql = f"""DROP TABLE IF EXISTS real_time_data_interval_{date_str};
        CREATE TABLE IF NOT EXISTS real_time_sum_interval_{date_str} (
                "timestamp" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ticker_id integer NOT NULL,
                current_price real,
                volume integer,
                ask_price_10 real,
                bid_price_1 real,
                ask_quantity_total integer,
                bid_quantity_total integer,
                CONSTRAINT real_time_sum_interval_{date_str}_ticker_id_fkey FOREIGN KEY (ticker_id)
                    REFERENCES public.ticker_code_mapping (ticker_id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            );
            
            CREATE INDEX IF NOT EXISTS idx_real_time_sum_interval_{date_str}_ticker_id
                ON real_time_sum_interval_{date_str} USING btree (ticker_id ASC NULLS LAST);
            
            CREATE INDEX IF NOT EXISTS idx_real_time_sum_interval_{date_str}_timestamp
                ON real_time_sum_interval_{date_str} USING btree ("timestamp" ASC NULLS LAST);"""
        cursor.execute(drop_sql)
        conn.commit()
        import_csv(f"real_time_sum_interval_{date_str}")
    cursor.close()
    conn.close()

def export_price_trends_to_csv():
    table_name = 'combine_trends_20240926_360'
    export_table_to_csv(table_name)

def create_and_insert_sql():
    dates = [
        (2024, 10, 11), (2024, 10, 16)
    ]

    conn = psycopg2.connect(**postgres_config)
    cur = conn.cursor()
    for date in dates:
        date_str = f"{date[0]:04d}{date[1]:02d}{date[2]:02d}"
        
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS real_time_sum_interval_{date_str} (
                "timestamp" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ticker_id integer NOT NULL,
                current_price real,
                volume integer,
                ask_price_10 real,
                bid_price_1 real,
                ask_quantity_total integer,
                bid_quantity_total integer,
                CONSTRAINT real_time_sum_interval_{date_str}_ticker_id_fkey FOREIGN KEY (ticker_id)
                    REFERENCES public.ticker_code_mapping (ticker_id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            );
            
            CREATE INDEX IF NOT EXISTS idx_real_time_sum_interval_{date_str}_ticker_id
                ON real_time_sum_interval_{date_str} USING btree (ticker_id ASC NULLS LAST);
            
            CREATE INDEX IF NOT EXISTS idx_real_time_sum_interval_{date_str}_timestamp
                ON real_time_sum_interval_{date_str} USING btree ("timestamp" ASC NULLS LAST);
        """

        insert_data_sql = f"""
            INSERT INTO real_time_sum_interval_{date_str} (
                "timestamp", 
                ticker_id, 
                current_price, 
                volume, 
                ask_price_10, 
                bid_price_1, 
                ask_quantity_total, 
                bid_quantity_total
            )
            SELECT 
                "timestamp", 
                ticker_id, 
                current_price, 
                volume, 
                ask_price_10, 
                bid_price_1, 
                -- Using COALESCE to treat NULL values as 0
                COALESCE(ask_quantity_1, 0) + COALESCE(ask_quantity_2, 0) + COALESCE(ask_quantity_3, 0) + 
                COALESCE(ask_quantity_4, 0) + COALESCE(ask_quantity_5, 0) + COALESCE(ask_quantity_6, 0) + 
                COALESCE(ask_quantity_7, 0) + COALESCE(ask_quantity_8, 0) + COALESCE(ask_quantity_9, 0) + 
                COALESCE(ask_quantity_10, 0) + COALESCE(ask_quantity_over, 0) + COALESCE(ask_quantity_market, 0) 
                AS ask_quantity_total,
                
                COALESCE(bid_quantity_1, 0) + COALESCE(bid_quantity_2, 0) + COALESCE(bid_quantity_3, 0) + 
                COALESCE(bid_quantity_4, 0) + COALESCE(bid_quantity_5, 0) + COALESCE(bid_quantity_6, 0) + 
                COALESCE(bid_quantity_7, 0) + COALESCE(bid_quantity_8, 0) + COALESCE(bid_quantity_9, 0) + 
                COALESCE(bid_quantity_10, 0) + COALESCE(bid_quantity_under, 0) + COALESCE(buy_quantity_market, 0)
                AS bid_quantity_total
            FROM real_time_data_interval_{date_str};
        """
    
        cur.execute(create_table_sql)
        cur.execute(insert_data_sql)
    conn.commit()
    cur.close()
    conn.close()

def export_import_all():
    os.makedirs(sql_directory, exist_ok=True)

    # dump_schema("stock", "stock", "localhost", os.path.join(sql_directory, "schema_only.sql"))
    restore_schema("stock", "stock", "localhost", 5432, os.path.join(sql_directory, "schema_only.sql"))

    # export_tables()
    # delete_from_tables()
    import_tables()
    # create_and_insert_sql()


if __name__ == "__main__":
    export_import_all()
