# MyPackage/Investar/DBUpdater.py
import sqlite3
import pandas as pd
from datetime import datetime

class DBUpdater:
    def __init__(self):
        '''생성자: SqliteDB 연결 및 종목코드 딕셔너리 생성'''
        self.conn = sqlite3.connect('../../DB/investar.db')
        cursor = self.conn.cursor()

        sql = '''
        CREATE TABLE IF NOT EXISTS company_info(
            code varchar(20),
            company varchar(40),
            last_update date,
            PRIMARY KEY(code)
        );
        '''
        cursor.execute(sql)

        sql = '''
        CREATE TABLE IF NOT EXISTS daily_price(
            code varchar(20),
            date date,
            open bigint(20),
            high bigint(20),
            low bigint(20),
            close bigint(20),
            diff bigint(20),
            volume bigint(20),
            PRIMARY KEY(code, date)
        );
        '''
        cursor.execute(sql)
        
        self.conn.commit()
        cursor.close()
        
        self.codes = dict()
        self.update_comp_info()
        
        
    def __del__(self):
        '''소멸자: DB연결 해제인데 필요할진 몰겟'''
        self.conn.close()
        
    
    def read_krx_code(self):
        '''KRX로부터 상장법인목록 파일을 읽어와서 데이터프레임으로 변환'''
        url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&pageIndex=1&currentPageSize=5000&comAbbrv=&beginIndex=&orderMode=3&orderStat=D&isurCd=&repIsuSrtCd=&searchCodeType=&marketType=&searchType=13&industry=&fiscalYearEnd=all&comAbbrvTmp=&location=all'
        
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드','회사명']]
        krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        
        return krx
        
    def update_comp_info(self):
        '''종목코드를 company_info 테이블에 업데이트 한 후 딕셔너리에 저장'''
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        cursor = self.conn.cursor()
        
        sql = "SELECT max(last_update) FROM company_info"
        cursor.execute(sql)
        rs = cursor.fetchone()
        today = datetime.today().strftime('%Y-%m-%d')

        if rs[0] == None or rs[0] < today:
            krx = self.read_krx_code()
            for idx in range(len(krx)):
                code = krx.code.values[idx]
                company = krx.company.values[idx]
                sql = f"REPLACE INTO company_info (code, company, last_update) VALUES('{code}','{company}','{today}')"
                cursor.execute(sql)

                self.codes[code] = company
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f'[{tmnow}] {idx:04d} REPLACE INTO company_info Values ({code}, {company}, {today})')
            self.conn.commit()
            print('')
        cursor.close()
        
        
    def read_naver(self, code, company, pages_to_fetch):
        '''네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE'''
        
    def update_daily_price(self, pages_to_fetch):
        '''KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트'''
        
    def execute_daily(self):
        '''실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트'''
        
if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
