import time
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta, timezone
from requests.exceptions import HTTPError
import requests
from bs4 import BeautifulSoup
import pytz


def validate_date(date_string):
    try:
        # This will raise ValueError if the format is incorrect
        parsed_date = datetime.strptime(date_string, '%Y-%m-%d')
        # Additional check to ensure the date is valid
        if parsed_date.year < 1 or parsed_date.year > datetime.now().year or parsed_date.month < 1 or parsed_date.month > 12 or parsed_date.day < 1 or parsed_date.day > 31 or len(date_string) != 10:
            raise Exception(ValueError(f"Invalid date: {date_string}"))
    except ValueError:
        raise Exception(ValueError(f"Invalid date: {date_string}"))

# Function to fetch aggregates with retry logic
def fetch_aggregates(client, ticker, multiplier, timespan, from_date, to_date, limit, max_retries=5):
    aggs = []
    retries = 0
    while retries < max_retries:
        try:
            for agg in client.list_aggs(ticker, multiplier, timespan, from_date, to_date, limit=limit):
                aggs.append(agg)
            break  # Exit loop if successful
        except HTTPError as e:
            if e.response.status_code == 429:  # Too Many Requests
                retries += 1
                wait_time = 2 ** retries  # Exponential backoff
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise e
    return aggs


def get_previous_business_day(date_str):
    current_date = datetime.strptime(date_str, "%Y-%m-%d")

    # Go back one day
    previous_day = current_date - timedelta(days=1)
    
    # Check if it's a weekend, adjust if necessary
    while previous_day.weekday() in [5, 6]:  # Saturday is 5, Sunday is 6
        previous_day -= timedelta(days=1)
    
    return previous_day.strftime("%Y-%m-%d")


def prepare_day_df(ticker, api_key, date):

    # Get previous close
    previous_date = get_previous_business_day(date)

    # Construct the URL for the API request
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{previous_date}/{previous_date}?apiKey={api_key}'

    # Send the request
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        if 'results' in data and len(data['results']) > 0:
            df = pd.DataFrame(data['results'])
            prev_close = df['c'][0]     
        else:
            print("No data found for the specified date.")
    else:
        print(f"Failed to fetch data: {response.status_code}")




    # Get minute-level data
    minute_data_url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?apiKey={api_key}'
    minute_data_response = requests.get(minute_data_url)

    if minute_data_response.status_code == 200:
        minute_data = minute_data_response.json()
        if 'results' in minute_data and len(minute_data['results']) > 0:
            # Extract the 'results' part of the data
            results = minute_data['results']

            # Convert the 'results' to a DataFrame
            minute_data = pd.DataFrame(results)
            
            # Define the desired time zone
            timezone = pytz.timezone('America/New_York')
            
            minute_data['time'] = pd.to_datetime(minute_data['t'], unit='ms', utc=True).dt.tz_convert(timezone).dt.tz_localize(None)
            
            # Define the start of the afternoon session
            afternoon_4 = pd.to_datetime(f'{date} 04:00:00').tz_localize(timezone).tz_localize(None)
            afternoon_9_29 = pd.to_datetime(f'{date} 09:29:00').tz_localize(timezone).tz_localize(None)
            afternoon_9_30 = pd.to_datetime(f'{date} 09:30:00').tz_localize(timezone).tz_localize(None)
            afternoon_end_16 = pd.to_datetime(f'{date} 16:00:00').tz_localize(timezone).tz_localize(None)
            afternoon_end_19 = pd.to_datetime(f'{date} 19:00:00').tz_localize(timezone).tz_localize(None)

            # Calculate PM Hi and its time
            afternoon_data = minute_data[(minute_data['time'] >= afternoon_4) & (minute_data['time'] <= afternoon_9_29)]
            pm_high = afternoon_data['h'].max()
            pm_high_time = afternoon_data.loc[afternoon_data['h'].idxmax(), 'time']
            
            # Calculate PM Low Post Hi
            post_high_data = minute_data[minute_data['time'] >= pm_high_time]
            pm_low_post_hi = post_high_data['l'].min()
            pm_low_post_hi_time = post_high_data.loc[post_high_data['l'].idxmin(), 'time']

            #Calculate PM total vol to PM High
            afternoon_data1 = minute_data[(minute_data['time'] >= afternoon_4) & (minute_data['time'] <= afternoon_9_29)]
            # Find the highest price during this time frame
            highest_price_idx = afternoon_data1['h'].idxmax()
            # Sum the volume of trades that occurred at the highest price
            total_volume_given_time = afternoon_data1.loc[:highest_price_idx, 'v'].sum()

            
            # Calculate PM Volume
            pm_vol = afternoon_data1['v'].sum()

            # Calculate Open Price
            open_price_df = minute_data[minute_data['time'] >= afternoon_9_30]
            open_price_given_time = open_price_df.iloc[0]['o']

            # Calculate Open 1 Minute Volume
            open_1_min_vol = open_price_df.iloc[0]['v']
            
            # Calculate Open 2 Minute Volume
            open_2_min_vol = open_price_df.iloc[0]['v'] + open_price_df.iloc[1]['v']

            # Identify key points
            open_price = minute_data.iloc[0]['o']
            intra_day_high = minute_data['h'].max()
            intra_day_high_time = minute_data[minute_data['h'] == intra_day_high]['time'].iloc[0]
            
            # Calculate ∑ VOL: PM to Intra-Day High
            post_pm_data = minute_data[(minute_data['time'] >= afternoon_4) & (minute_data['time'] <= afternoon_end_16)]
            highest_price_intra_day = post_pm_data['h'].max()
            highest_price_idx_intra_day = post_pm_data['h'].idxmax()
            volume_pm_to_intra_hi = post_pm_data.loc[:highest_price_idx_intra_day, 'v'].sum()
            
            # Calculate ∑ VOL: Open to Intra-Day High
            post_pm_data_open = minute_data[(minute_data['time'] >= afternoon_9_29) & (minute_data['time'] <= afternoon_end_16)]
            highest_price_intra_day_open = post_pm_data_open['h'].max()
            highest_price_idx_intra_day_open = post_pm_data_open['h'].idxmax()
            volume_open_to_intra_hi = post_pm_data_open.loc[:highest_price_idx_intra_day_open, 'v'].sum()
            
            # Calculate 2m + 50% Gap Price
            two_min_high = minute_data.iloc[:2]['h'].max()
            gap = open_price - prev_close
            gap_50_percent = 0.5 * gap
            two_min_plus_50_gap_price = two_min_high + gap_50_percent
            two_min_plus_50_gap_time = minute_data['time'][minute_data.iloc[:2]['h'].idxmax()]

            # Calculate HOD (High of the Day)
            hod_data = minute_data[(minute_data['time'] >= afternoon_9_29) & (minute_data['time'] <= afternoon_end_16)]
            hod = hod_data['h'].max()
            hod_time = hod_data.loc[hod_data['h'].idxmax(), 'time']

            # Calculate HOD % change from open price
            hod_percent_change = ((hod - open_price) / open_price) * 100

            # Calculate HOD 1 Minute Volume
            highest_price_hod = hod_data['h'].max()
            highest_price_idx_hod = hod_data['h'].idxmax()
            hod_1_min_vol = hod_data.loc[:highest_price_idx_hod, 'v']

            # Calculate EOD Price and % change from open pricezz
            eod_price = minute_data[minute_data['time'] >= afternoon_end_16].iloc[0]['o']
            eod_percent_change = ((eod_price - open_price) / open_price) * 100

            # Calculate Total Volume for the Day
            total_volume_data = minute_data[(minute_data['time'] >= afternoon_4) & (minute_data['time'] <= afternoon_end_19)]
            total_volume = total_volume_data['v'].sum()

            data_dict = {
                "Date" : date,
                "Day" : pd.to_datetime(date).day_name(),
                "Ticker" : ticker,
                "PREV Close" : prev_close,
                "PM Hi" : pm_high,
                "PM Hi Time" : pm_high_time,
                "PM total vol to PM High" : total_volume_given_time,
                "PM Volume" : pm_vol,
                "Open $" : open_price_given_time,
                "Open 1 min Vol" : open_1_min_vol,
                "Open 2 min Vol" : open_2_min_vol,
                "∑ VOL: PM High to Intra-Day High" : volume_pm_to_intra_hi,
                "∑ VOL: Open to Intra-Day High" : volume_open_to_intra_hi,
                "HOD" : hod,
                "HOD Time" : hod_time,
                "EOD $" : eod_price,
                "Volume" : total_volume,
            }
            
            # # Print the results
            # print(f"Date: {date}")
            # print(f"Day: {pd.to_datetime(date).day_name()}")
            # print(f"Ticker: {ticker}")
            # print(f"Previous Close: {prev_close}")
            # print(f"PM High: {pm_high} at {pm_high_time}")
            # print(f"OH High: {oh_high} at {oh_high_time}")
            # print(f"PM Low Post Hi: {pm_low_post_hi} at {pm_low_post_hi_time}")
            # print(f"PM Volume: {pm_vol}")
            # print(f"Open Price: {open_price}")
            # print(f"Open 1 Minute Volume: {open_1_min_vol}")
            # print(f"∑ VOL: PM High to Intra-Day High: {volume_pm_to_intra_hi}")
            # print(f"∑ VOL: Open to Intra-Day High: {volume_open_to_intra_hi}")
            # print(f"2m + 50% Gap Price: {two_min_plus_50_gap_price} at {two_min_plus_50_gap_time}")
            # print(f"HOD: {hod} at {hod_time}")
            # print(f"HOD %: {hod_percent_change:.2f}%")
            # print(f"HOD 1 Minute Volume: {hod_1_min_vol}")
            # print(f"LOD Pre Hi: {lod_pre_hi} at {lod_pre_hi_time}")
            # print(f"LOD Post Hi: {lod_post_hi} at {lod_post_hi_time}")
            # print(f"EOD %: {eod_percent_change:.2f}%")
            # print(f"Total Volume: {total_volume}")
            
            
            return data_dict
        else:
            print("No minute-level data found.")
    else:
        print(f"Failed to fetch minute-level data: {minute_data_response.status_code}")




# Function to prepare the minute-wise DataFrame
def prepare_minute_df(ticker, date, api_key):

    # Construct the URL for the API request
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?apiKey={api_key}'

    # Send the request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        if 'results' in data and len(data['results']) > 0:
            # Define the desired time zone
            timezone = pytz.timezone('America/New_York')
            # Loop through the results and extract the desired values
            data_dict = [{
                # Convert timestamp to datetime in UTC and then to the desired time zone
                'time': pd.to_datetime(result['t'], unit='ms', utc=True).tz_convert(timezone).tz_localize(None),
                'open_price' : result['o'],  # Open price
                'close_price' : result['c'],  # Close price
                'high_price' : result['h'],  # High price
                'low_price' : result['l'],  # Low price
                'volume' : result['v'],  # Volume
            }for result in data['results']]
            return pd.DataFrame(data_dict)
                

                # print(f"Time: {time}, Open: {open_price}, Close: {close_price}, High: {high_price}, Low: {low_price}, Volume: {volume}")
        else:
            print("No results found in the response.")
    else:
        print(f"Failed to fetch data: {response.status_code}")

def fetch_all_tickers(api_key):
    
    url = 'https://api.polygon.io/v3/reference/tickers'
    params = {
        'apiKey': api_key,
        'limit': 1000  # Number of results per page
    }
    
    tickers = []
    next_url = url
    
    while next_url:
        response = requests.get(next_url, params=params)
        if response.status_code == 200:
            data = response.json()
            tickers.extend([ticker['ticker'] for ticker in data['results']])
            next_url = data['next_url'] if 'next_url' in data else None
        else:
            print(f"Failed to fetch data: {response.status_code}")
            break
    
    return tickers



# Function to get the webpage content
def get_webpage_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Failed to fetch the webpage. Status code: {response.status_code}")


# Function to parse the webpage and extract data
def extract_data(content):
    soup = BeautifulSoup(content, 'html.parser')
    data = {}

    # Extracting specific values
    try:
        data['Market Cap'] = soup.find(string='Market Cap').find_next('td').text.strip()
    except:
        data['Market Cap'] = "###"
    # print(data['Market Cap'])
    try:
        data['Float'] = soup.find(string='Shs Float').find_next('td').text.strip()
    except:
        data['Float'] = "###"
    try:
        data['Short Float %'] = soup.find(string='Short Float').find_next('td').text.strip()
    except:
        data['Short Float %'] = "###"
    try:
        data['Shares O/S'] = soup.find(string='Shs Outstand').find_next('td').text.strip()
    except:
        data['Shares O/S'] = "###"
    try:
        data['Inst Own %'] = soup.find(string='Inst Own').find_next('td').text.strip()
    except:
        data['Inst Own %'] = "###"
    try:
        data['Insider Own'] = soup.find(string='Insider Own').find_next('td').text.strip()
    except:
        data['Insider Own'] = "###"
    return data

# Function to scrape data for a single ticker
def scrape_ticker(ticker):
    url = f'https://www.finviz.com/quote.ashx?t={ticker}'
    content = get_webpage_content(url)
    data = extract_data(content)
    return data




if __name__ == "__main__":
    
    # Your Polygon API key
    api_key = 'uZrGN5iyiIoNfDfWewnVLhss2SXwNQ82'

    # Initialize the RESTClient with your API key
    client = RESTClient(api_key)
    
    # Initialize Tickers
    print("Choose Option for Tickers")
    print("Press 0 for Select all Tickers OR Press 1 for Select Target Tickers")
    option = int(input())
    if option == 0:
        tickers = fetch_all_tickers(api_key)
    else:
        print("Enter Ticker Names with Space Seperated")
        tickers = input().upper().split()
    
    print("Enter date in year-month-date(YYYY-MM-DD) format")

    # Enter Date
    try:
        date = input()
        validate_date(date)
    except ValueError as e:
        print(e)
    
    # Create a dictionary to store minute-wise DataFrames for each ticker
    minute_data_dfs = {}

    # Create a dictionary to store day-wise DataFrames for each ticker
    day_data_list = []
    
    # Create a dictionary to store finviz DataFrames for each ticker
    finviz_data = []
    
    # Fetch data for each ticker and update DataFrames
    for ticker in tickers:
        
    
        minute_data_df = prepare_minute_df(ticker, date, api_key)
        
        day_data = prepare_day_df(ticker, api_key, date)
        day_data_list.append(day_data)
        
        # day_data_df = pd.concat([day_data_df, df_fenviz], axis=1)
        
        minute_data_dfs[ticker] = minute_data_df
        
        try:
            data = scrape_ticker(ticker)
            finviz_data.append(data)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
        
    day_data_df = pd.DataFrame(day_data_list)
    finviz_data_df = pd.DataFrame(finviz_data)
    
    combine_data_df = pd.concat([day_data_df,finviz_data_df], axis=1)
       
    # Save the DataFrames to an Excel workbook with multiple sheets
    try:
        output_file_path = 'Updated_Stock_Data.xlsx'
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            # Write the aggregate data
            combine_data_df.to_excel(writer, sheet_name='Aggregated Data', index=False)
            # Write minute-wise data for each ticker
            for ticker, minute_data in minute_data_dfs.items():
                minute_data.to_excel(writer, sheet_name=ticker, index=False)
    except:
        raise Exception("Not data found")
            
    print("Data updated successfully!")
