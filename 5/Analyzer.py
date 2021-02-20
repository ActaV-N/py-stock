import pymysql, re
import pandas as pd
from datetime import datetime, timedelta

class MarketDB:
    def __init__(self):
        '''생성자 MariaDB 연결 및 종목코드 딕셔너리 생성'''
        self.conn = pymysql.connect(host='localhost', user='root', password='****', db='Investar', charset='utf8')
        self.codes = {}
        self.get_comp_info()
        
    
    def __del__(self):
        '''소멸자: MariaDB 연결 해제'''
        self.conn.close()
        
        
    def get_comp_info(self):
        '''Comapny Info 불러와서 딕셔너리에 저장'''
        sql = 'SELECT * FROM company_info'
        df = pd.read_sql(sql, self.conn)
        
        for idx in range(len(df)):
            code = df.code.values[idx]
            company = df.company.values[idx]
            
            self.codes[code] = company
    
    
    def get_daily_price(self, code, start_date=None, end_date=None):
        '''
        KRX 종목의 일별시세를 데이터프레임 형태로 변환
        - code       : KRX 종목코드('005930') 또는 상장기업명('삼성전자')
        - start_date : 조회 시작일('2020-01-01'), 미입력 시 1년 전 오늘
        - end_date   : 조회 종료일('2020-12-31'), 미입력 시 오늘 날짜
        '''
        
        if start_date is None:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print(f"start_date is initialized to '{start_date}'")
        else:
            start_list = re.split('\D+', start_date)
            if start_list[0] == '':
                start_list = start_list[1:]
                
            start_year = int(start_list[0])
            start_month = int(start_list[1])
            start_day = int(start_list[2])
            
            if start_year < 1800 or start_year>2200:
                print(f"Value Error: start_year({start_year:d}) is wrong.")
                return
            if start_month < 1 or start_month > 12:
                print(f"Value Error: start_moth({start_month:d}) is wrong")
                return
            if start_day < 1 or start_day > 31:
                print(f"Value Error: start_day({start_day:d}) is wrong")
                return
            
            start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"
            
        if end_date is None:
            end_date = datetime.today().stftime('%Y-%m-%d')
            print(f"end_date is initialized to '{end_date}'")
        else:
            end_list = re.split('\D+', end_date)
            if end_list[0] == '':
                end_list = end_list[1:]
                
            end_year = int(end_list[0])
            end_month = int(end_list[1])
            end_day = int(end_list[2])
            
            if end_year < 1800 or end_year>2200:
                print(f"Value Error: end_year({end_year:d}) is wrong.")
                return
            if end_month < 1 or end_month > 12:
                print(f"Value Error: end_month({end_month:d}) is wrong")
                return
            if end_day < 1 or end_day > 31:
                print(f"Value Error: end_day({end_day:d}) is wrong")
                return 
            
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"
            
        code_keys = list(self.codes.keys())
        code_values = list(self.codes.values())
        if code in code_keys:
            pass
        elif code in code_values:
            idx = code_values.index(code)
            code = code_keys[idx]
        else:
            print(f"ValueError: Code({code}) doesn't exist")
            
        sql = f"SELECT * FROM daily_price WHERE code = '{code}' and date >= '{start_date}' and date <= '{end_date}'"
        df = pd.read_sql(sql, self.conn)
        df.index = df['date']
        return df
     
            
