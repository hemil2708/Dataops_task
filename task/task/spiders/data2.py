import scrapy
from scrapy.cmdline import execute
import xml.etree.ElementTree as ET
import pandas as pd
import requests
import pymongo
class DataSpider(scrapy.Spider):
    name = 'data2'
    allowed_domains = ['www.example.com']
    start_urls = ['http://www.example.com/']
    lol = []
    counter = 0
    host = 'localhost'
    port = '27017'
    db = 'epv_gov_task'

    def parse(self,response):
        try:
            url = "https://www.certipedia.com/search/certified_companies?locale=en"

            payload = {}
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
                'Cookie': 'TS01a8edcd=011d3f5e829c64452d8497088ace9ce4d1ea0d211f401cc790d3f640cffb09df2028a4558f22fa0bbb08507c8d91e4608e0c94d83c2185d19463cb799b9ef90a1d71b84cc7; _tuvdotcom2_session=UHQwSlpqbkdvQWpKR0xxYWVvUXpxSFdoUW1uMXRXOHlFb0RKUEdvUGNxeGtReC9vMkw3dW5BcHRiWWF2ZFJEYlJZdDFPcEZIY0R3WkR4NHUxaHpXcG1LcHFaWThLaXdWNFN5eHRwV2J2TWhtSVN1YnNQTzQyVHBzQ2wrNFMyNTgyNlRCRWNmeEQ0TG5neFRXNUVHclBBWWNEWkp2UjlyS3RrVi8wcGFWakJXSmpKbno0bmlMNlFRSTd3ODZSNEJ0LS1uOVBpa0U4dENPeUk1c1NHTk1QV2V3PT0%3D--720851701c6b8ee7f106f07e28fd55d85db96a1b'
            }
            yield scrapy.Request(url=url,headers=headers,callback=self.firstlevel,dont_filter=True)
        except Exception as e:
            print(e)
    def firstlevel(self,resposne):
        try:
            div = resposne.xpath('//*[@class="pager-alpha starting_letters"]//li//a//@href|//*[@class="pager-alpha starting_letter_pairs"]//li//a//@href').extract()
            for i in div:
                url = "https://www.certipedia.com" + i
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6,gu;q=0.5',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
                    'Cookie': 'TS01a8edcd=011d3f5e829c64452d8497088ace9ce4d1ea0d211f401cc790d3f640cffb09df2028a4558f22fa0bbb08507c8d91e4608e0c94d83c2185d19463cb799b9ef90a1d71b84cc7; _tuvdotcom2_session=UHQwSlpqbkdvQWpKR0xxYWVvUXpxSFdoUW1uMXRXOHlFb0RKUEdvUGNxeGtReC9vMkw3dW5BcHRiWWF2ZFJEYlJZdDFPcEZIY0R3WkR4NHUxaHpXcG1LcHFaWThLaXdWNFN5eHRwV2J2TWhtSVN1YnNQTzQyVHBzQ2wrNFMyNTgyNlRCRWNmeEQ0TG5neFRXNUVHclBBWWNEWkp2UjlyS3RrVi8wcGFWakJXSmpKbno0bmlMNlFRSTd3ODZSNEJ0LS1uOVBpa0U4dENPeUk1c1NHTk1QV2V3PT0%3D--720851701c6b8ee7f106f07e28fd55d85db96a1b'
                }
                yield scrapy.FormRequest(url=url,headers=headers,callback=self.secondlevel,dont_filter=True)
        except Exception as e:
            print(e)

    def secondlevel(self,response):
        try:
            body = response.xpath('//*[@class="links search-results"]//a/@href').extract()
            for i in body:
                link = "https://www.certipedia.com" + i
                yield scrapy.Request(url=link,callback=self.thirdlevel,dont_filter=True)
            next = "https://www.certipedia.com" + response.xpath('//*[contains(text(),"Next")]//@href').extract_first(default="")
            yield scrapy.Request(url=next,callback=self.secondlevel, dont_filter=True)
        except Exception as e:
            print(e)

    def thirdlevel(self,response):
        try:
            data = "https://www.certipedia.com" + response.xpath('//*[contains(text(),"Further Information")]/..//@href').extract_first(default="")
            yield scrapy.Request(url=data, callback=self.fourthlevel, dont_filter=True)
        except Exception as e:
            print(e)

    def fourthlevel(self,response):
        try:
            final_link = response.xpath('//*[@class="search-results"]//td[3]//a/@href').extract()
            for i in final_link:
                linkk = "https://www.certipedia.com" + i
                yield scrapy.Request(url=linkk, callback=self.parsing, dont_filter=True)
        except Exception as e:
            print(e)
    def parsing(self,response):
        con = pymongo.MongoClient(f'mongodb://{self.host}:{self.port}/')
        mydb = con[self.db]
        conn = mydb['epvgov']
        try:
            try:
                certificate_holder = response.xpath('//*[@class="quality_mark_header content-sub content-sub-first"]/h1[3]//text()').extract_first(default="").replace("Certificate Holder:","").strip()
            except:
                certificate_holder = ""
            try:
                mark_number = response.xpath('//*[@class="quality_mark_header content-sub content-sub-first"]/h1[4]//text()').extract_first(default="").split(":")[-1].strip()
            except:
                mark_number = ""
            try:
                quality_logo = "https://www.certipedia.com" + response.xpath('//*[@class="quality_mark_logo"]//img/@src').extract_first(default="").strip()
            except:
                quality_logo = ""
            try:
                des = ''.join(response.xpath('//*[@class="keyword_section"]//text()').extract()).strip()
            except:
                des = ""
            try:
                certificate_scope = ''.join(response.xpath('//*[@id="certificate_type_scopes"]//text()').extract()).strip()
            except:
                certificate_scope = ""
            try:
                certificate_data = ''.join(response.xpath('//*[contains(text(),"Show certificate data:")]/../..//td[2]//text()').extract()).strip()
            except:
                certificate_data = ""
            try:
                certificate_holder_address = ''.join(response.xpath('//*[@id="contact_to_the_certificate_holder"]//td[2]//text()').extract()).strip()
            except:
                certificate_holder_address = ""


            item = {}
            item['certificate_holder'] = certificate_holder
            item['mark_number'] = mark_number
            item['quality_logo'] = quality_logo
            item['Descriptions'] = des
            item['certificate_scope'] = certificate_scope
            item['certificate_data'] = certificate_data
            item['certificate_holder_address'] = certificate_holder_address
            item['Final_url'] = response.url
            try:
                # conn.create_index("Final_url", unique=True)
                X = conn.insert(dict(item))
                print("Data Inserted Succesfully..!!")
            except Exception as e:
                print(e, "Please Check Your Coding")
        except Exception as e:
            print(e)

if __name__ == '__main__':
    execute("scrapy crawl data2".split())