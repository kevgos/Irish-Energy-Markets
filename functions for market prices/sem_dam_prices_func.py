import pandas as pd # import pandas package to make dataframe
import requests # requests module for API interaction
import csv
from urllib.request import urlopen
import re
from datetime import datetime 
import pytz # reports are in UTC, need to put in to local time Europe/Dublin so daylight savings is observed

def DAM_price_from_API(start_date, end_date):
    """Returns a csv file with SEM DAM prices between the specified start_date and end_date in UTC"""
    # create an empty list to add all the dataframes to
    list_of_dfs = []
    
    # API Step 1: Query Report List
    api_url = "https://reports.semopx.com/api/v1/documents/static-reports"
    query_params = {
    "Group": "Market Data",
    "Date": f'>={start_date}<={end_date}', #<=
    "page_size": 500, # Adjust as needed based on the expected number of reports per day
    "DPuG_ID": "EA-001", # market result ID
    "sort_by": "PublishTime", }
   
    response_semo = requests.get(api_url, params=query_params)
    report_list_semo = response_semo.json()

    # from the report list filter for DAM and then get the report name to get the data
    for item in report_list_semo['items']:
        if re.findall(r'MarketResult_SEM-DA', item['ResourceName']):
            prices_dict = {}
            report_name = item['ResourceName'] # assign the name of the DAM bid ask curve file to this variable
            #print(report_name)
            individ_report_url = f"https://reports.semopx.com/documents/{report_name}"
            #print(individ_report_url)

            # use urlopen package and csv reader (both built-in) to read through the url linking csv data
            response = urlopen(individ_report_url)
            #print(response)
            lines = [line.decode('utf-8') for line in response.readlines()]
            csv_reader = csv.reader(lines, delimiter=';')
            for number, row in enumerate(csv_reader):
                if number == 7:
                    prices_dict['Datetime(UTC)'] = row
                if number == 8:
                    prices_dict['€/MWh'] = row
                if number == 11:
                    prices_dict['GBP/MWh'] = row
                else:
                    continue
            prices_df = pd.DataFrame(prices_dict)
            list_of_dfs.append(prices_df)
            final_df = pd.concat(list_of_dfs)
    # change the date to local time for Europe/Dublin from UTC, keep the old UTC column for now too
    final_df['Datetime(Dublin)'] = final_df['Datetime(UTC)'].str.replace('Z', '')
    final_df['Datetime(Dublin)'] = final_df['Datetime(Dublin)'].str.replace('T', ' ')
    final_df['Datetime(Dublin)'] = pd.to_datetime(final_df['Datetime(Dublin)'])
    local_timezone = pytz.timezone('Europe/Dublin')  # assign local timezone to be Europe/Dublin
    final_df['Datetime(Dublin)'] = final_df['Datetime(Dublin)'].dt.tz_localize('UTC').dt.tz_convert(local_timezone)
    final_df['Datetime(Dublin)'] = final_df['Datetime(Dublin)'].dt.strftime('%d-%m-%Y %H:%M')
    final_df['Market'] = 'SEM-DA'
    return(final_df) # return the final pandas dataframe of hourly prices

# example usage, specify dates
my_start = '2023-12-26'
my_end = '2024-02-14'

#give arguments to function
test_df = DAM_price_from_API(my_start, my_end)

# save the resulting pandas dataframe as a local csv file
test_df.to_csv("market_price_Example.csv", sep = ',', index = False)