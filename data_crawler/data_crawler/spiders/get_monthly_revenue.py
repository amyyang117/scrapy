import scrapy
import pandas as pd
import datetime
import logging
from io import StringIO
from ..items import UniformCrawlerItem , RevenueCrawlerItem

class get_monthly_revenue(scrapy.Spider):
    name = 'get_monthly_revenue'
    allowed_domains = ['mops.twse.com.tw']

    # start_urls = ['https://mops.twse.com.tw/nas/t21/']
    markets = ['sii', 'otc','rotc']
    def __init__(self, year, month, *args, **kargs):
        super(get_monthly_revenue, self).__init__(*args, **kargs)
        
        self.year = year
        self.month = month
    def start_requests(self):
        logging.debug("Starting requests...")
        markets = ['sii', 'otc']
        kys = [0,1]
        for market in markets:
            for ky in kys:
                self.url = f'https://mops.twse.com.tw/nas/t21/{market}/t21sc03_{self.year}_{self.month}_{ky}.html'
                yield scrapy.Request(self.url, self.parse_stock)
    
    def parse_stock(self, response, **kwargs):
        dfs = pd.read_html(StringIO(response.text))
        for df in dfs:
            if len(df)>1:
                try:
                    df.columns = df.columns.droplevel(0)
                    columns = ['公司 代號', '當月營收', '上月比較 增減(%)', '去年同月 增減(%)', '當月累計營收', '前期比較 增減(%)', '備註' ]
                    df = df[columns]
                    df.columns = ['code', 'revenue', 'mom', 'yoy', 'cum_revenue', 'cum_yoy', 'note']
                    df = df[df['code']!='合計']
                    value = df.to_dict('records')

                    items = UniformCrawlerItem()
                    items['date'] = pd.to_datetime(f"{int(self.year)+1911}-{self.month}")
                    items['parse_date'] = datetime.date.today()
                    items['table'] = 'monthly_revenue'
                    items['status'] = 'success' if response.status == 200 else 'error'
                    items['items'] = list()

                    for data in value:
                        val = RevenueCrawlerItem()
                        for k, v in data.items():
                            val[k] = v
                        items['items'].append(dict(val))
                        
                    yield dict(items)
                
                except Exception as e:
                    print(e)
                # yield {
                #     'table_data': df.to_dict(orient='records')
                # }
        
        # return super().parse(response, **kwargs)
# https://mops.twse.com.tw/nas/t21/sii/t21sc03_112_8_0.html