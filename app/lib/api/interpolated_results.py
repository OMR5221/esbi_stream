import requests
from requests.auth import HTTPBasicAuth
import json
# Parse response for the needed values to load into STG1 table:
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date

class GetInterpolatedValues():

    def __init__(self, server_name, point_name, start_time_str, end_time_str, interval):
        self.url = 'http://dummy.api.source.url'
        self.headers = {'content-type': 'text/xml'}
        self.body = """
       <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:fpl="http://apisource/">
       <soapenv:Header/>
       <soapenv:Body>
          <fpl:GetInterpolatedValues>
            <!--Optional:-->
                <fpl:serverName>{ServerName}</fpl:serverName>
            <!--Optional:-->
                <fpl:pointName>{PointName}</fpl:pointName>
            <!--Optional:-->
                <fpl:startTime>{StartTime}</fpl:startTime>
            <!--Optional:-->
                <fpl:endTime>{EndTime}</fpl:endTime>
                <fpl:intervals>{Interval}</fpl:intervals>
          </fpl:GetInterpolatedValues>
       </soapenv:Body>
    </soapenv:Envelope>
    """.format(ServerName=server_name, PointName=point_name, StartTime=start_time_str,
           EndTime=end_time_str, Interval=interval)


    def post(self):
        response = requests.post(self.url, data=self.body, headers=self.headers)
        xml = response.content
        soup = BeautifulSoup(xml, 'lxml')
        return soup
        # [{'Timestamp': rec['time'], 'Value': rec.value.text} for rec in soup.findAll('values')]
