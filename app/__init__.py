from bs4 import BeautifulSoup
from lxml import html
import asyncio, aiohttp, csv, time, re
from datetime import datetime
import threading, os, pprint
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class CompanyBs4(threading.Thread):
    def __init__(self, url, group):
        threading.Thread.__init__(self)
        self.daemon = True
        self.stop_event = threading.Event()
        self.currentValue = '//div[@data-test="instrument-price-last"]'
        self.variantionValue = '//span[@data-test="instrument-price-change"]'
        self.variantionPercent = '//div[contains(@class, "flex") and contains(@class, "items-center")]/span[@data-test="instrument-price-change-percent"]'
        self.variationDaily = '//div[@class="text-xs/4 flex-1 mb-2.5"]/div[contains(@class, "flex") and contains(@class, "items-center")]/span'
        self.url = url
        self.group = group

    def run(self): 
        try:
            asyncio.run(self.process(self.url))
        except Exception as e:
            print(f"Erro: {e}")
            self.stop_thread()
            self.selenium_thread = CompanySelenium(self.url, self.group)
            self.selenium_thread.start()

    async def fetchPage(self, session, url):
        start_time = time.time()
        async with session.get(url) as response:
            if response.status == 200:
                html_content = await response.text()
                self.duration = time.time() - start_time
                soup = BeautifulSoup(html_content, 'html.parser')
                document = html.fromstring(soup.prettify())
                return document
            else:
                print(f"Erro: {response.status} - {url}")
                self.stop_thread()
                self.selenium_thread = CompanySelenium(url, self.group)
                self.selenium_thread.start()

    async def getPage(self, url):
        async with aiohttp.ClientSession() as session:
            task = self.fetchPage(session, url)
            html = await asyncio.gather(task)
        return html[0]

    async def treatmentData(self, document, company, values=None):
        if values is None:
            variantionPercent = document.xpath(self.variantionPercent)[0].text_content().strip()
            variantionPercent = variantionPercent.split('\n')[2].strip()
            variationDaily = [element.text_content().strip() for element in document.xpath(self.variationDaily)]
            data = {
                "currentValue": document.xpath(self.currentValue)[0].text_content().strip(),
                "variantionValue":  document.xpath(self.variantionValue)[0].text_content().strip(),
                "variantionPercent": variantionPercent,
                "variationDailyInit": variationDaily[0],
                "variationDailyInitNow": variationDaily[1],
                "DateTime": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "timeRequest": self.duration
            }
        else:
            data = {
                "currentValue": values[0],
                "variantionValue": values[1],
                "variantionPercent": values[2],
                "variationDailyInit": values[3],
                "variationDailyInitNow": values[4],
                "DateTime": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "timeRequest": self.duration
            }
        await self.saveData(list(data.values()), list(data.keys()), company)
        
    async def saveData(self, data, headers, company):
        if not os.path.exists(f"data/{self.group}"):
            os.makedirs(f"data/{self.group}")

        mode = 'a'
        write_headers = False

        try:
            with open(f"data/{self.group}/{company}.csv", 'r') as f:
                pass
        except FileNotFoundError:
            write_headers = True

        with open(f"data/{self.group}/{company}.csv", mode, newline='') as f:
            writer = csv.writer(f)
            if write_headers and headers:
                writer.writerow(headers)
            writer.writerow(data)
    
    async def process(self, url):
         while True:
            document = await self.getPage(url)
            lines = document.xpath('//tbody/tr[contains(@class, "datatable-v2")]')
            for key, line in enumerate(lines):
                company = line.xpath('//td/div/a')[key].text_content().strip().replace('/', '-')
                line = line.text_content()
                values = [f"'{value}'".replace(",", ".") for value in re.findall(r'[+-]?[\d.,]+', line)]
                await self.treatmentData(None, company, values)
            await asyncio.sleep(60) 

    def stop_thread(self):
        self.stop_event.set()

class CompanySelenium(threading.Thread):
    def __init__(self, url, group):
        threading.Thread.__init__(self)
        options = webdriver.ChromeOptions()
        # options.add_argument("--headless")
        options.add_argument("--start-maximized")
        self.chrome = webdriver.Chrome(options = options)
        self.url = url
        self.group = group

    def run(self):
        try:
            self.chrome.get(self.url)
            lineXpath = '//tbody/tr[contains(@class, "datatable-v2")]'
            elements = asyncio.run(self.getElements(lineXpath, True))
            data = asyncio.run(self.treatmentData(elements))
            pprint.pprint(data)
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            self.chrome.quit()

    async def treatmentData(self, elements):
        print("Tratando data")
        dataReturn = list()
        for element in elements:
            companyData = element.find_elements(By.XPATH, '//td[contains (@class, "datatable-v2_cell__IwP1U")]')
            linha = []
            for data in companyData:
                texto = data.text.strip()
                linha.append(texto if texto else None)
            dataReturn.append(linha)
        return dataReturn
    
    async def getElements(self, xpath, multiple=False):
        self.search_element(xpath)
        if multiple is True:
            return self.chrome.find_elements(By.XPATH, xpath)
        return self.chrome.find_element(By.XPATH, xpath)
    
    async def saveData(self, data, headers, company):
        if not os.path.exists(f"data/{self.group}"):
            os.makedirs(f"data/{self.group}")

        mode = 'a'
        write_headers = False

        try:
            with open(f"data/{self.group}/{company}.csv", 'r') as f:
                pass
        except FileNotFoundError:
            write_headers = True

        with open(f"data/{self.group}/{company}.csv", mode, newline='') as f:
            writer = csv.writer(f)
            if write_headers and headers:
                writer.writerow(headers)
            writer.writerow(data)
    
    def search_element(self, element):
        while len(self.chrome.find_elements(By.XPATH, element)) == 0:
            time.sleep(0.01)

async def main():
    links = {
        # "indices": "https://br.investing.com/indices/major-indices",
        # "commodities": "https://br.investing.com/commodities/real-time-futures",
        "criptos": "https://br.investing.com/crypto/",
    }
    
    threads = list()
    for key, url in links.items():
        # company = CompanyBs4(url, key)
        company = CompanySelenium(url, key)
        company.start()
        threads.append(company)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    asyncio.run(main())