import requests
from scrapy.http import HtmlResponse
import re
import scrapy
from scrapy.cmdline import execute
import xml.etree.ElementTree as ET
import pandas as pd
class DataSpider(scrapy.Spider):
    name = 'data'
    allowed_domains = ['www.example.com']
    start_urls = ['http://www.example.com/']
    lol = []
    counter = 0
    def parse(self,resposne):
        try:

            url = "https://www.epa.gov/system/files/other-files/2021-07/toplists.xml"

            payload = {}
            headers = {
                'accept': 'application/xml, text/xml, */*; q=0.01',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'
            }
            res = requests.get(url,headers=headers,data=payload)
            data = ET.fromstring(res.content)
            for i in data.iter('TopLists'):
                try:
                    for partner_name in i.findall('Name'):
                        partner_name = partner_name.text
                except:
                    partner_name = ""
                try:
                    for kwh in i.findall('KWh'):
                        kwh = kwh.text
                except:
                    kwh = ""
                try:
                    for gp in i.findall('PercentGP'):
                        gp = gp.text
                except:
                    gp = ""
                try:
                    for Industry in i.findall('OrgType'):
                        Industry = Industry.text
                except:
                    Industry = ""
                try:
                    for GPResources in i.findall('GPResources'):
                        GPResources = GPResources.text
                except:
                    GPResources = ""
                item = {}
                item['partner_name'] = partner_name
                item['Annual Green Power Usage (kWh)'] = kwh
                item['GP % of Total Electricity Use*'] = gp
                item['Industry'] = Industry
                item['Green Power Resources'] = GPResources
                self.lol.append(item)
                self.counter = self.counter + 1
                if self.counter == 100:
                    break
            df = pd.DataFrame(self.lol)
            df.to_csv('epa.gov.csv',index=True)
            print("Csv created")
        except Exception as e:
            print(e)


if __name__ == '__main__':
    execute("scrapy crawl data".split())
