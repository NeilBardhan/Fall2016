import os
import io
import time
import json
import datetime
import requests
import pandas as pd

MAX_ATTEMPTS = 5
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


def download_data(url):
    """

    Args:
        url:

    Returns:
        pandas dataframe
    """

    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print("\tRequest Successful!")
                print("\tDownloading Data ... ")
                df = pd.read_csv(io.StringIO(response.text), header='infer')
                return df
            else:
                print("\tRequest failed ::")
                print("\t" + url)
        except Exception as exp:
            print(exp)
            time.sleep(5)
        attempt += 1
    print("\tMAX_ATTEMPTS Reached")
    return None


def main():
    """Main method"""
    # timestamps in UTC to request data for
    startts = datetime.datetime(2010, 1, 1)
    endts = datetime.datetime(2019, 12, 31)
    stations = ['NYC', 'JFK', 'LGA', 'JRB']
    # stations = ['NYC']
    prefix = '&data='
    suffix = 'tz=America%2FNew_York&format=onlycomma&latlon=yes&missing=null&trace=null&direct=no&report_type=1&report_type=2'
    dataParams = ['tmpc', 'dwpc', 'relh', 'feel', 'mslp', 'p01m', 'metar']
    for station in stations:
        print("Station ->", station)
        url = SERVICE + 'station=' + station + ''.join([prefix + mystr for mystr in dataParams]) + \
              startts.strftime("&year1=%Y&month1=%m&day1=%d&") + endts.strftime("year2=%Y&month2=%m&day2=%d&") + \
              suffix
        # print(url)
        call = download_data(url)
        if call is not None:
            print("\tWriting Data ... ")
            call.to_csv(os.getcwd() + "//data//" + station + '_wMETAR.csv', index=False)
            print("\tWriting Successful")
        else:
            print("\tStation :", station, "failed.")


if __name__ == "__main__":
    main()
