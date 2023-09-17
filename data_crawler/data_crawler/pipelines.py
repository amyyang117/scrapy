#pipeline

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2


class DataCrawlerPipeline:
    def __init__(self):
        ## Connection Details
        hostname = 'localhost'
        portname = '5432'
        username = 'postgres'
        password = 'amyyang17'
        database = 'postgres'

        ## Create/Connect to database
        self.connection = psycopg2.connect(host=hostname, port=portname, user=username, password=password, dbname=database)
        
        ## Create cursor, used to execute commands
        self.cur = self.connection.cursor()



    def process_item(self, data, spider):
        date = data['date']
        table = data['table']
        items = data['items']

        try:
            for item in items:
                item['date'] = date
                cols = ', '.join(item.keys())
                placeholders = ', '.join(['%s'] * len(item))

                # Extract values from the items dictionary
                values = [item[i] for i in item]
                insert_query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
                self.cur.execute(insert_query, values)

                ## Execute insert of data into database
                self.connection.commit()
            return data
        except Exception as e:
            print(e)

            
    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.connection.close()