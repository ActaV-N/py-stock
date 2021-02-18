import pandas as pd
import urllib.request as urllib
import pymysql, calendar, time, json
from datetime import datetime
from bs4 import BeautifulSoup

class DBUpdater:
    def __init__(self):
        '''Initialize sqltie and table'''
        self.conn = pymysql.connect(host='localhost', user='root', password='****', db='Investar', charset='utf8')
        
        with self.conn.cursor() as curs:
            sql = '''
                CREATE TABLE IF NOT EXISTS company_info(
                    code varchar(20),
                    company varchar(40),
                    last_update date,
                    PRIMARY KEY(code)
                )
            '''
            curs.execute(sql)

            sql = '''
                CREATE TABLE IF NOT EXISTS daily_price(
                    code varchar(20),
                    date date,
                    oepn bigint(20),
                    high bigint(20),
                    low bigint(20),
                    close bigint(20),
                    diff bigint(20),
                    volume bigint(20),
                    PRIMARY KEY(code, date)
                )
            '''
            curs.execute(sql)
        self.conn.commit()
        
        self.codes = {}
        
    
    def __del__(self):
        '''Close connection'''
        self.conn.close()
        
    
    def read_krx_code(self):
        '''Read krx data from krx site and return dataframe'''
        krx = pd.read_html('https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&pageIndex=1&currentPageSize=5000&comAbbrv=&beginIndex=&orderMode=3&orderStat=D&isurCd=&repIsuSrtCd=&searchCodeType=&marketType=&searchType=13&industry=&fiscalYearEnd=all&comAbbrvTmp=&location=all')[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx['code'] = krx['code'].map('{:06d}'.format)
        
        return krx
    
    
    def update_company_info(self):
        '''Update company_info table'''
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        
        for idx in range(len(df)):
            self.codes[df.code.values[idx]] = df.company.values[idx]
        
        with self.conn.cursor() as curs:
            sql = 'SELECT max(last_update) from company_info'
            curs.execute(sql)

            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0] < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]

                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')

                    sql = f"REPLACE INTO company_info(code, company, last_update) VALUES('{code}', '{company}', '{today}')"
                    curs.execute(sql)

                    self.codes[code] = company

                    print(f"[{tmnow}] #{idx:04d} {company} ({code}) : REPLACE INTO company_info({code},{company},{today})")
                self.conn.commit()
                print('')
                    
                
    def read_naver(self, code, company, pages_to_fetch):
        '''Read OHLC prices from naver finance and return dataframe'''
        try:
            url = f'https://finance.naver.com/item/sise_day.nhn?code={code}'

            opener = urllib.build_opener()
            opener.addheaders = [('User-Agent','Mozilla/5.0')]

            with opener.open(url) as doc:
                if doc is None:
                    return None
                
                html = BeautifulSoup(doc, 'lxml')
                pgrr = html.find('td', class_='pgRR')
                
                if pgrr is None:
                    return None
                
                s = pgrr.a['href'].split('=')

                last_page = s[-1]

            pages = min(int(last_page), pages_to_fetch)
            
            df = pd.DataFrame()
            for page in range(1, pages + 1):
                pageUrl = f'{url}&page={page}'

                df = df.append(pd.read_html(opener.open(pageUrl).read(), header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                
                print(f"[{tmnow}] {company} ({code}) : {page:04d} / {pages:04d} pages are downloading...")        
                
            df = df.rename(columns={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high','저가':'low','거래량':'volume'})
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)

            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occured: ',str(e))
            return None
            
        return df
            
        
    def replace_into_db(self, df, code, company, num):
        '''Replace into daily_price with data from naver finance'''
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES('{code}', '{r.date}', {r.open},{r.high}, {r.low}, {r.close}, {r.diff}, {r.volume})"
                curs.execute(sql)

            self.conn.commit()
        
        tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
        print(f"[{tmnow}] #{num:04d} {company} ({code}): {len(df)}rows > REPLACE INTO DB")
        
        
    def update_daily_price(self, pages_to_fetch):
        '''Update daily_price with data from krx'''
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, code, self.codes[code], idx)
        
    def execute_daily(self):
        '''Execute daily'''
        self.update_company_info()
        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch':1}
                json.dump(config, out_file)
                
        self.update_daily_price(pages_to_fetch)
        
        tmnow = datetime.now()
        last_day = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == last_day:
            tmnext = tmnow.replace(year=tmnow.year+1, month=1, day=1, hour=17, minute=0, second=0)
        elif tmnow.day == last_day:
            tmnext = tmnow.replace(month = tmnow.month + 1, day=1, hour=17, minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0,second=0)
            
        secs = (tmnext - tmnow).seconds
        t = Timer(secs, self.excute_daily)
        print(f'Waiting for next update {tmnext.strftime('%Y-%m-%d %H:%M')}')
        t.start()
        
if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
