import psycopg2
import psycopg2.extras
import pandas as  pd
from db_credentials import postgres_config

cur = None
conn = None

try:
    conn = psycopg2.connect(host=postgres_config['hostname'],
                dbname=postgres_config['database'],
                user=postgres_config['username'],
                password=postgres_config['pwd'],
                port=postgres_config['port_id']
            )


    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def data_collect():
        cur.execute(''' SELECT store, "Date", customers FROM public.store as store ''')
        data = cur.fetchall()
        data = pd.DataFrame(data, columns=['store', 'date', 'customers'])
        return df

    def data_transform(df):

        df['date'] = pd.to_datetime(df['date'])

        df_transformed = df[df['store'] == 2]

        return df_transformed


     
    df = data_collect()

    df_transformed = data_transform(df)


    conn.commit()    
    print(df_transformed)
    print('task done')
except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
    if cur is not None:
        cur.close()
