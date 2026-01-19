import requests
import pandas as pd
import time
from typing import List, Dict
from datetime import datetime, timedelta

# Configuration
API_KEY = "8a1040db1b22242be8b67004ab80c4ebedb64bb61c3ad38a556b0b43009a2afe"
HEADERS = {"X-API-Key": API_KEY} if API_KEY != "YOUR_API_KEY" else {}
BASE_URL = "https://api.openaq.org/v3"
NIGERIA_COUNTRY_ID = 100

# Rate limiting configuration
REQUESTS_PER_MINUTE = 60
REQUESTS_PER_HOUR = 2000
MIN_DELAY_BETWEEN_REQUESTS = 1.0  # seconds (60 requests/min = 1 req/sec)

class RateLimiter:
    """Rate limiter to manage API calls within limits"""
    def __init__(self, requests_per_minute=60, requests_per_hour=2000):
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.minute_requests = []
        self.hour_requests = []
        self.last_request_time = None
        
    def wait_if_needed(self):
        """Wait if necessary to stay within rate limits"""
        current_time = datetime.now()
        
        # Remove old requests outside the time windows
        minute_ago = current_time - timedelta(minutes=1)
        hour_ago = current_time - timedelta(hours=1)
        
        self.minute_requests = [t for t in self.minute_requests if t > minute_ago]
        self.hour_requests = [t for t in self.hour_requests if t > hour_ago]
        
        # Check hourly limit
        if len(self.hour_requests) >= self.requests_per_hour:
            oldest_request = min(self.hour_requests)
            wait_time = 3600 - (current_time - oldest_request).total_seconds()
            if wait_time > 0:
                print(f"\n⏳ Hourly limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time + 1)
                current_time = datetime.now()
                self.hour_requests = []
        
        # Check minute limit
        if len(self.minute_requests) >= self.requests_per_minute:
            oldest_request = min(self.minute_requests)
            wait_time = 60 - (current_time - oldest_request).total_seconds()
            if wait_time > 0:
                print(f"\n⏳ Minute limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time + 1)
                current_time = datetime.now()
                self.minute_requests = []
        
        # Ensure minimum delay between requests
        if self.last_request_time:
            elapsed = (current_time - self.last_request_time).total_seconds()
            if elapsed < MIN_DELAY_BETWEEN_REQUESTS:
                time.sleep(MIN_DELAY_BETWEEN_REQUESTS - elapsed)
                current_time = datetime.now()
        
        # Record this request
        self.minute_requests.append(current_time)
        self.hour_requests.append(current_time)
        self.last_request_time = current_time
        
    def get_stats(self):
        """Get current rate limit usage statistics"""
        current_time = datetime.now()
        minute_ago = current_time - timedelta(minutes=1)
        hour_ago = current_time - timedelta(hours=1)
        
        minute_count = len([t for t in self.minute_requests if t > minute_ago])
        hour_count = len([t for t in self.hour_requests if t > hour_ago])
        
        return {
            'minute': f"{minute_count}/{self.requests_per_minute}",
            'hour': f"{hour_count}/{self.requests_per_hour}"
        }

# Initialize rate limiter
rate_limiter = RateLimiter(REQUESTS_PER_MINUTE, REQUESTS_PER_HOUR)

def make_api_request(url, params):
    """Make an API request with rate limiting"""
    rate_limiter.wait_if_needed()
    
    try:
        response = requests.get(url, params=params, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        # If rate limited by server, wait and retry
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
            print("Server rate limit hit. Waiting 60 seconds before retry...")
            time.sleep(60)
            response = requests.get(url, params=params, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        raise

def get_locations_in_nigeria() -> List[Dict]:
    """Get all monitoring locations in Nigeria"""
    print(f"Fetching locations for Nigeria (ID: {NIGERIA_COUNTRY_ID})...")
    url = f"{BASE_URL}/locations"
    params = {
        "countries_id": NIGERIA_COUNTRY_ID,
        "limit": 1000,
        "page": 1
    }
    
    all_locations = []
    
    try:
        while True:
            stats = rate_limiter.get_stats()
            print(f"Rate limits - Min: {stats['minute']}, Hour: {stats['hour']}")
            
            data = make_api_request(url, params)
            
            locations = data.get('results', [])
            all_locations.extend(locations)
            
            print(f"Retrieved {len(locations)} locations (Page {params['page']})")
            
            # Check if there are more pages
            if len(locations) < params['limit']:
                break
            
            params['page'] += 1
            
    except Exception as e:
        print(f"Error fetching locations: {e}")
    
    print(f"Total locations found: {len(all_locations)}")
    return all_locations

def get_sensor_daily_data(sensor_id: int, sensor_name: str, parameter_name: str) -> List[Dict]:
    """Get daily aggregated data for a specific sensor"""
    url = f"{BASE_URL}/sensors/{sensor_id}/days"
    params = {"limit": 1000, "page": 1}
    
    all_days = []
    
    try:
        while True:
            data = make_api_request(url, params)
            
            days = data.get('results', [])
            all_days.extend(days)
            
            # Check if there are more pages
            if len(days) < params['limit']:
                break
            
            params['page'] += 1
            
    except Exception as e:
        print(f"Error fetching data for sensor {sensor_id} ({parameter_name}): {e}")
    
    return all_days

def fetch_nigeria_data() -> pd.DataFrame:
    """Main function to fetch all Nigeria air quality data"""
    
    start_time = datetime.now()
    print(f"Starting data collection at {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get all locations in Nigeria
    locations = get_locations_in_nigeria()
    
    if not locations:
        print("No locations found in Nigeria")
        return pd.DataFrame()
    
    # Iterate through sensors and collect daily data
    all_rows = []
    total_sensors = sum(len(loc.get('sensors', [])) for loc in locations)
    processed = 0
    
    print(f"\nProcessing {total_sensors} sensors from {len(locations)} locations...")
    print("This may take a while due to rate limiting...\n")
    
    for location in locations:
        city = location.get('name', 'Unknown')
        country_name = location.get('country', {}).get('name', 'Nigeria')
        
        for sensor in location.get('sensors', []):
            sensor_id = sensor['id']
            parameter = sensor.get('parameter', {})
            specie = parameter.get('name', 'unknown')
            
            processed += 1
            stats = rate_limiter.get_stats()
            print(f"[{processed}/{total_sensors}] Sensor {sensor_id} ({specie}) at {city} | Limits - Min: {stats['minute']}, Hour: {stats['hour']}")
            
            # Fetch daily aggregated data
            daily_data = get_sensor_daily_data(sensor_id, sensor['name'], specie)
            
            for day in daily_data:
                summary = day.get('summary', {})
                period = day.get('period', {})
                
                # Extract date from period
                datetime_info = period.get('datetimeFrom', {})
                date_str = datetime_info.get('local', datetime_info.get('utc', ''))
                date = date_str.split('T')[0] if date_str else 'Unknown'
                
                # Calculate variance from standard deviation
                sd = summary.get('sd')
                variance = sd ** 2 if sd is not None else 0.0
                
                row = {
                    "Date": date,
                    "Country": country_name,
                    "City": city,
                    "Specie": specie,
                    "count": summary.get('count', 0),
                    "min": summary.get('min', 0.0),
                    "max": summary.get('max', 0.0),
                    "median": summary.get('median', 0.0),
                    "variance": variance
                }
                all_rows.append(row)
    
    # Create DataFrame
    print("\nCreating DataFrame...")
    df = pd.DataFrame(all_rows)
    
    if not df.empty:
        # Ensure correct data types
        df['Date'] = df['Date'].astype('object')
        df['Country'] = df['Country'].astype('object')
        df['City'] = df['City'].astype('object')
        df['Specie'] = df['Specie'].astype('object')
        df['count'] = df['count'].astype('int64')
        df['min'] = df['min'].astype('float64')
        df['max'] = df['max'].astype('float64')
        df['median'] = df['median'].astype('float64')
        df['variance'] = df['variance'].astype('float64')
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "="*60)
        print("DATA COLLECTION COMPLETE")
        print("="*60)
        print(f"\nTotal execution time: {duration}")
        print(f"Total records: {len(df)}")
        print(f"\nDataFrame Info:")
        df.info()
        print(f"\nFirst 10 rows:")
        print(df.head(10))
        print(f"\nDate range: {df['Date'].min()} to {df['Date'].max()}")
        print(f"\nSpecies found: {df['Specie'].unique()}")
        print(f"\nCities found: {df['City'].nunique()}")
        
        # Save to CSV
        output_file = 'nigeria_air_quality_data.csv'
        df.to_csv(output_file, index=False)
        print(f"\nData saved to: {output_file}")
    else:
        print("No data found.")
    
    return df

# Execute the data collection
if __name__ == "__main__":
    df = fetch_nigeria_data()