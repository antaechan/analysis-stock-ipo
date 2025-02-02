from selenium import webdriver
import pandas as pd
import time
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import yfinance as yf
import FinanceDataReader as fdr
import re
from pandas.tseries.offsets import BDay


def get_IPO_DATA(stock_url):
    def find(table_name, row_loc, col_loc):
        try:
            table = soup.find("table", {"summary": table_name})
            if not table:
                print("공모청약일정 테이블을 찾을 수 없음.")

            result = (
                table.find_all("tr")[row_loc]
                .find_all("td")[col_loc]
                .get_text(strip=True)
            )
            return result
        except:
            return None

    try:
        response = requests.get(stock_url, verify=False, timeout=5)
        time.sleep(1)

        soup = BeautifulSoup(response.text, "html.parser")

        ticker = find("기업개요", 1, -1)
        price_band = find("공모정보", 2, 1)

        table = soup.find("table", {"summary": "공모청약일정"})
        if not table:
            print("공모청약일정 테이블을 찾을 수 없음.")

        texts = []
        for row in table.find_all("tr"):
            for col in row.find_all("td"):
                texts.append(col.get_text(strip=True))

        competition_rate = texts[texts.index("기관경쟁률") + 1]

    except:
        ticker = None
        price_band = None
        competition_rate = None

    shares = None
    try:
        naver_url = f"https://finance.naver.com/item/fchart.naver?code={ticker}"
        response = requests.get(naver_url, verify=False, timeout=5)
        time.sleep(1)

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"summary": "시가총액 정보"})
        shares = table.find_all("tr")[2].find("td").get_text(strip=True)
    except:
        pass

    return ticker, price_band, competition_rate, shares


def get_OHLCV(ticker, date):
    start_date = datetime.strptime(date, "%Y-%m-%d")
    end_date = start_date + BDay(20)
    start_date = date    
    
    try:
        ohlcv_data = fdr.DataReader(ticker, start_date, end_date)
        if ohlcv_data.empty:
            return None, None, None, None, None
        
        open = ohlcv_data.loc[start_date, "Open"]
        high = ohlcv_data.loc[start_date, "High"]
        low = ohlcv_data.loc[start_date, "Low"]
        close = ohlcv_data.loc[start_date, "Close"]
        return_20 = ohlcv_data["Close"][-1]
        

    except:
        open = None
        high = None
        low = None
        close = None
        return_20 = None

    return open, high, low, close, return_20


def crawl(base_url, max_page):
    IPO_DATABASE = []  # 데이터를 저장할 리스트

    # 페이지 번호를 순차적으로 탐색
    page = 2
    while page < max_page:
        url = f"{base_url}&page={page}"

        driver = webdriver.Chrome()

        # 크롬 드라이버에 url 주소 넣고 실행
        driver.get(url)
        time.sleep(1)

        html = driver.page_source  # URL에 해당하는 페이지의 HTML를 가져옴
        soup = BeautifulSoup(html, "html.parser")

        # Extract data
        table = soup.find("table", {"summary": "신규상장종목"})
        if not table:
            print("테이블을 찾을 수 없음.")

        rows = table.find_all("tr")
        if len(rows) <= 1:  # 데이터가 없으면 종료
            print("더 이상 데이터 없음.")

        # 데이터 추출
        for row in rows[1:]:  # 첫 번째 행은 헤더이므로 제외
            cols = row.find_all("td")
            if len(cols) < 5:  # 잘못된 행 스킵
                continue

            # 종목코드 추출
            a_tag = cols[0].find("a", href=True)
            stock_url = a_tag["href"].replace("./", "http://www.38.co.kr/html/fund/")
            ticker, price_band, competition_rate, shares = get_IPO_DATA(stock_url)

            start_date = cols[1].get_text(strip=True)
            start_date = datetime.strptime(start_date, "%Y/%m/%d")
            start_date = start_date.strftime("%Y-%m-%d")

            open = cols[6].get_text(strip=True).replace("-", "")
            close = cols[8].get_text(strip=True).replace("-", "")
            OHLCV = get_OHLCV(ticker, start_date)

            if close == "":
                close = OHLCV[3]

            if open == "":
                open = OHLCV[0]

            high = OHLCV[1]
            low = OHLCV[2]
            return_20 = OHLCV[4]

            IPO_DATA = {
                "기업명": cols[0].get_text(strip=True),
                "종목코드": ticker,
                "신규상장일": start_date,
                "경쟁률": competition_rate,
                "상장주식수": shares,
                "공모가 밴드": price_band,
                "공모가(원)": cols[4].get_text(strip=True),
                "시초가(원)": open,
                "시초/공모(%)": cols[7].get_text(strip=True),
                "첫날종가(원)": close,
                "상장일 고가": high,
                "상장일 저가": low,
                "20일후 종가": return_20,
            }

            IPO_DATABASE.append(IPO_DATA)

        driver.quit()

        # 다음 페이지로 이동
        page += 1

    return IPO_DATABASE


if __name__ == "__main__":

    BASE_URL = "https://www.38.co.kr/html/fund/index.htm?o=nw"  # 공모주 URL
    max_page = 24  # 크롤링할 최대 페이지 수

    IPO_DATABASE = crawl(BASE_URL, max_page)

    # pandas DataFrame으로 변환
    database = pd.DataFrame(IPO_DATABASE)

    # CSV 파일로 저장
    csv_file = "database.csv"
    database.to_csv(csv_file, index=False, encoding="utf-8-sig")
