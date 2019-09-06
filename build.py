import json
import pandas as pd
import psycopg2 as pg
import fitpy as fp
import matplotlib.pyplot as plt





with open('var/creds/rds-creds.json', 'r') as f:
    rds_creds = json.load(f)

host = rds_creds['host']
port = rds_creds['port']
database = rds_creds['database']
rds_user_id = rds_creds['user_id']
password = rds_creds['password']





conn = pg.connect(host=host, port=port, database=database, user=rds_user_id, password=password)
cur = conn.cursor()

query = ("select * from sleep " + 
"where userid = '7BVHQT' " + 
"and \"end\" >= date '2019-04-15' " + 
"order by start;")
cur.execute(query)
colnames = [cn[0] for cn in cur.description]
data = cur.fetchall()

# Preprocess
sleep_df = pd.DataFrame(data, columns=colnames)


## stitch & filter
sleep = fp.SleepPreprocessor()
sleep.fit(sleep_df)
trans_sleep_df = sleep.transform(stitch=True, filter='night_main')

# print(trans_sleep_df)


plt.hist(trans_sleep_df['bed'])
plt.show()






