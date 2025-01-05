from bs4 import BeautifulSoup
from lxml import html
import asyncio, aiohttp, csv, time, re
from datetime import datetime
import pprint

class Company():
    def __init__(self):
        self.currentValue = '//div[@data-test="instrument-price-last"]'
        self.variantionValue = '//span[@data-test="instrument-price-change"]'
        self.variantionPercent = '//div[contains(@class, "flex") and contains(@class, "items-center")]/span[@data-test="instrument-price-change-percent"]'
        self.variationDaily = '//div[@class="text-xs/4 flex-1 mb-2.5"]/div[contains(@class, "flex") and contains(@class, "items-center")]/span'

    async def fetchPage(self, session, url):
        start_time = time.time()
        async with session.get(url) as response:
            if response.status == 200:
                html_content = await response.text()
                self.duration = time.time() - start_time
                soup = BeautifulSoup(html_content, 'html.parser')
                title = soup.title.string.strip() if soup.title else ""
                document = html.fromstring(soup.prettify())
                if title != 'Just a moment...':
                    return document
            await asyncio.sleep(1)

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
        mode = 'a'
        write_headers = False

        try:
            with open(f"data/{company}.csv", 'r') as f:
                pass
        except FileNotFoundError:
            write_headers = True

        with open(f"data/{company}.csv", mode, newline='') as f:
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
                values = re.findall(r'[\d.,-]+', line)
                await self.treatmentData(None, company, values)
            await asyncio.sleep(60) 
    
async def main():
    links = {
        "indices": "https://br.investing.com/indices/major-indices"
    }
    
    threads = list()
    for key, url in links.items():
        company = Company()
        await company.process(url)

if __name__ == "__main__":
    asyncio.run(main())
