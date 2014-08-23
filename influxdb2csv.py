#!/usr/bin/env python

import urllib
import argparse
import json
import sys

def parseArguments():
    parser = argparse.ArgumentParser(description="This script will execute an influxdb query and convert it into CSV.")
    parser.add_argument("--host", help="InfluxDB server name/IP", default="localhost")
    parser.add_argument("--port", help="InfluxDB server port", default=8086, type=int)
    parser.add_argument("--user", help="InfluxDB user name", default=None)
    parser.add_argument("--pwd", help="InfluxDB password", default=None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--http", help="Uses http to connect to InfluxDB", action="store_true", default=True)
    group.add_argument("-https", help="Uses https to connect to InfluxDB", action="store_true")
    parser.add_argument("db", help="InfluxDB database name")
    parser.add_argument("query", help="Query to execute")
    return parser.parse_args()

def formURL(parserArgs):
    url = ""
    
    if parserArgs.http:
        url = "http://"
    else:
        url = "https://"

    url = url + parserArgs.host + ":" + str(parserArgs.port) + "/db/" + parserArgs.db + "/series?"

    if parserArgs.user is not None: url = url + "u=" + parserArgs.user + "&"
    if parserArgs.pwd is not None: url = url + "p=" + parserArgs.pwd + "&"

    url = url + urllib.urlencode({"q" : parserArgs.query})

    return url

def fetchResult(url):
    fid = urllib.urlopen(url)
    jsonData = fid.read()
    fid.close()

    return json.loads(jsonData)

def translateResult(jsonData):
    cols = jsonData[0]["columns"]
    points = jsonData[0]["points"]

    finalAns = ""
    for col in cols:
        finalAns = finalAns + "\"" + col + "\","
    finalAns = finalAns[:-1]
    print finalAns

    for point in points:
        finalAns = ""
        for index in point:
            if type(index) is int:
                finalAns += str(index) + ","
            elif type(index) is float:
                finalAns += str(index) + ","
            else:
                finalAns += "\"" + index + "\"" + ","
        finalAns = finalAns[:-1] 
        print finalAns

def main():
    parserArgs = parseArguments()
    url = formURL(parserArgs)
    jsonData = fetchResult(url)
    translateResult(jsonData)

if __name__ == "__main__":
    main()