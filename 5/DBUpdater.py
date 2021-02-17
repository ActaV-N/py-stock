import pandas as pd
import urllib.request as urllib
import sqlite3

class DBUpdater:
    def __init__(self):
        '''Initialize sqltie and table'''
        self.conn = sqlite3.connect('investar.db')
        curs = self.conn.cursor()
        
        sql = '''
            CREATE TABLE IF NOT EXISTS company_info(
                code varchar(20),
                copmany varchar(40),
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
        krx = self.read_krx_code()
        
        for idx in range(len(krx)):
            self.codes[krx.code.values[idx]] = krx.copmany.values[idx]
        
        sql = 'SELECT max(last_update) from company_info'
        
        
if __name__ == '__main__':
    dbu = DBUpdater()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    