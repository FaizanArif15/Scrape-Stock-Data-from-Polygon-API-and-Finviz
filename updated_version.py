import time
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta, timezone
from requests.exceptions import HTTPError
import requests
from bs4 import BeautifulSoup


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
        # print(data)
        if 'results' in data and len(data['results']) > 0:
            df = pd.DataFrame(data['results'])
            prev_close = df['c'][0]     
            # print(prev_close)    
            # print(f"Previous Close for {previous_date}: {closing_price}")
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
            df = pd.DataFrame(minute_data['results'])
            df['time'] = pd.to_datetime(df['t'], unit='ms')
            # print(df)
            # Define the start of the afternoon session
            afternoon_start = pd.to_datetime(f'{date} 12:00:00')

            # Calculate PM Hi
            afternoon_data = df[df['time'] >= afternoon_start]
            pm_high = afternoon_data['h'].max()
            pm_high_time = afternoon_data.loc[afternoon_data['h'].idxmax(), 'time']

            # Calculate PM Low Post Hi
            post_high_data = df[df['time'] >= pm_high_time]
            pm_low_post_hi = post_high_data['l'].min()
            pm_low_post_hi_time = post_high_data.loc[post_high_data['l'].idxmin(), 'time']

            # Calculate PM Volume
            pm_vol = afternoon_data['v'].sum()

            # Calculate Open Price
            open_price = df.iloc[0]['o']

            # Calculate Open 1 Minute Volume
            open_1_min_vol = df.iloc[0]['v']

            # Identify key points
            open_price = df.iloc[0]['o']
            intra_day_high = df['h'].max()
            intra_day_high_time = df[df['h'] == intra_day_high]['time'].iloc[0]
            
            # PM High and its time
            afternoon_start = pd.to_datetime(f'{date} 12:00:00')
            afternoon_data = df[df['time'] >= afternoon_start]
            pm_high = afternoon_data['h'].max()
            pm_high_time = afternoon_data[afternoon_data['h'] == pm_high]['time'].iloc[0]

            # Calculate ∑ VOL: PM High to Intra-Day High
            post_pm_data = df[df['time'] >= pm_high_time]
            volume_pm_to_intra_hi = post_pm_data[post_pm_data['time'] <= intra_day_high_time]['v'].sum()
            
            # Calculate ∑ VOL: Open to Intra-Day High
            volume_open_to_intra_hi = df[df['time'] <= intra_day_high_time]['v'].sum()

            
            # Calculate 2m + 50% Gap Price
            two_min_high = df.iloc[:2]['h'].max()
            gap = open_price - prev_close
            gap_50_percent = 0.5 * gap
            two_min_plus_50_gap_price = two_min_high + gap_50_percent
            two_min_plus_50_gap_time = df['time'][df.iloc[:2]['h'].idxmax()]

            # Calculate HOD (High of the Day)
            hod = df['h'].max()
            hod_time = df.loc[df['h'].idxmax(), 'time']

            # Calculate HOD % change from open price
            hod_percent_change = ((hod - open_price) / open_price) * 100

            # Calculate HOD 1 Minute Volume
            hod_1_min_vol = df.loc[df['h'].idxmax(), 'v']

            # Calculate LOD Pre Hi (Low of the Day before PM High)
            pre_high_data = df[df['time'] < pm_high_time]
            lod_pre_hi = pre_high_data['l'].min()
            lod_pre_hi_time = pre_high_data.loc[pre_high_data['l'].idxmin(), 'time']

            # Calculate LOD Post Hi (Low of the Day after PM High)
            lod_post_hi = post_high_data['l'].min()
            lod_post_hi_time = post_high_data.loc[post_high_data['l'].idxmin(), 'time']

            # Calculate EOD % change from open price
            eod_price = df.iloc[-1]['c']
            eod_percent_change = ((eod_price - open_price) / open_price) * 100

            # Calculate Total Volume for the Day
            total_volume = df['v'].sum()

            data_dict = {
                "Date" : date,
                "Day" : pd.to_datetime(date).day_name(),
                "Ticker" : ticker,
                "PREV Close" : prev_close,
                "PM Hi" : pm_high,
                "PM Hi Time" : pm_high_time,
                "PM Low-Post Hi" : pm_low_post_hi, 
                "PM Low-Post Hi Time" : pm_low_post_hi_time,
                "PM Volume" : pm_vol,
                "Open $" : open_price,
                "Open 1 min Vol" : open_1_min_vol,
                "∑ VOL: PM High to Intra-Day High" : volume_pm_to_intra_hi,
                "∑ VOL: Open to Intra-Day High" : volume_open_to_intra_hi,
                "2m + 50% Gap Price" : two_min_plus_50_gap_price,
                "2m + 50% Gap Price Time" : two_min_plus_50_gap_time,
                "HOD" : hod,
                "HOD Time" : hod_time,
                "HOD %" : hod_percent_change,
                "HOD 1 Minute Volume" : hod_1_min_vol,
                "LOD Pre Hi" : lod_pre_hi,
                "LOD Pre Hi Time" : lod_pre_hi_time,
                "LOD Post Hi" : lod_post_hi,
                "LOD Post Hi Time" : lod_post_hi_time,
                "EOD %" : eod_percent_change,
                "Total Volume" : total_volume,
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
            # Loop through the results and extract the desired values
            data_dict = [{
                # Convert timestamp to readable date-time if needed
                'time' : pd.to_datetime(result['t'], unit='ms'),
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
    api_key = 'Type your API Key'

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
            for ticker, df in minute_data_dfs.items():
                df.to_excel(writer, sheet_name=ticker, index=False)
    except:
        raise Exception("Not data found")
            
    print("Data updated successfully!")
