
import requests
import pandas as pd
import psycopg2
import os

from dotenv import load_dotenv

load_dotenv()


url= f'https://newsapi.org/v2/top-headlines?country=us&apiKey=1e0040592ec646739ac8a08a6302ce4b'
response= requests.get(url)
data= response.json()
my_data=data['articles']
df=pd.json_normalize(my_data)



#TRANSFORMATION
#news_df= df[['author', 'title', 'description', 'url', 'publishedAt','content', 'source.name']]
news_df= df.drop(['urlToImage','source.id'], axis=1)
news_df.rename(columns={'source.name': 'source'}, inplace=True)
news_df['publishedAt']= pd.to_datetime(news_df['publishedAt'])
news_df['content']= df['content'].str.replace('\n', '')
news_df['content']= df['content'].str.replace('\r', '')
news_df.fillna('Null', inplace = True)





#enforcing data types before loading
news_df= news_df.astype({
    'author': 'string',
    'title' : 'string',
    'description' : 'string',
    'url' : 'string', 
    'content' : 'string',
    'source' : 'string'
})

# conn= None
#cursor=None



try:
    conn= psycopg2.connect(
        host= "server4.postgres.database.azure.com",
        dbname= 'postgres',
        user= 'server4',
        password= os.getenv('DB_PASSWORD'),
        port= '5432'
    )


    cursor = conn.cursor()

    for index, row in news_df.iterrows():
        cursor.execute(
            '''INSERT INTO mynews (author, title, description, url, publishedAt, content,source)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (url) DO NOTHING;''',
            (row['author'], row['title'], row['description'], row['url'], row['publishedAt'], row['content'], row['source'] )    
        )

    conn.commit()
    print('DATA SUCCESFULLY INSERTED')
except Exception as e:
    print('Error inserting data:', e)

finally:
    cursor.close()
    conn.close()




