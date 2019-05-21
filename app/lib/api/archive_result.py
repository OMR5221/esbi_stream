# Will get the data from the API:
import requests
from requests.auth import HTTPBasicAuth
import json
# Parse response for the needed values to load into STG1 table:
from bs4 import BeautifulSoup

class GetArchiveValue():

    def __init__(self, server_name, point_name, start_time):
        self.url = 'http://dummy.api.url'
        self.headers = {'content-type': 'text/xml'}
        self.body = """
       <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:fpl="http://api.source/">
       <soapenv:Header/>
       <soapenv:Body>
          <fpl:GetArchiveValue>
             <!--Optional:-->
             <fpl:sServerName>{ServerName}</fpl:sServerName>
             <!--Optional:-->
             <fpl:sPointName>{PointName}</fpl:sPointName>
             <!--Optional:-->
             <fpl:sTimeStamp>{StartTime}</fpl:sTimeStamp>
          </fpl:GetArchiveValue>
       </soapenv:Body>
    </soapenv:Envelope>
    """.format(ServerName=server_name, PointName=point_name, StartTime=start_time)

    def post(self):
        response = requests.post(self.url, data=self.body, headers=self.headers)
        xml = response.content
        soup = BeautifulSoup(xml, 'lxml')
        return soup.body.value.text
