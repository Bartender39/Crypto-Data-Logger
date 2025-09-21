import requests
import pandas as pd
from datetime import datetime
import pytz
import os
import json
import time

def get_fear_greed_index():
    """Get Fear & Greed Index from Alternative.me"""
    try:
        response = requests.get('https://api.alternative.me/fng/')
        data = response.json()
        return data['data'][0]['value']
    except Exception as e:
        print(f"Error fetching Fear & Greed Index: {e}")
        return 'N/A'

def get_crypto_prices():
    """Get crypto prices with fallback"""
    prices = {'bitcoin': 'N/A', 'ethereum': 'N/A', 'solana': 'N/A'}
    
    # Try CoinGecko first
    try:
        response = requests.get(
            'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd',
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return {
                'bitcoin': data['bitcoin']['usd'],
                'ethereum': data['ethereum']['usd'],
                'solana': data['solana']['usd']
            }
    except Exception as e:
        print(f"CoinGecko failed: {e}")
    
    # Fallback to Binance
    try:
        print("Trying Binance fallback...")
        response = requests.get('https://api.binance.com/api/v3/ticker/price', timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            btc_data = next((item for item in data if item['symbol'] == 'BTCUSDT'), None)
            eth_data = next((item for item in data if item['symbol'] == 'ETHUSDT'), None)
            sol_data = next((item for item in data if item['symbol'] == 'SOLUSDT'), None)
            
            if btc_data:
                prices['bitcoin'] = float(btc_data['price'])
            if eth_data:
                prices['ethereum'] = float(eth_data['price'])
            if sol_data:
                prices['solana'] = float(sol_data['price'])
                
    except Exception as e:
        print(f"Binance fallback failed: {e}")
    
    return prices

def get_funding_rates():
    """Get funding rates with multiple fallbacks"""
    funding_rates = {'BTC': 'N/A', 'ETH': 'N/A', 'SOL': 'N/A'}
    
    # Try CoinGlass API first (free, no auth)
    try:
        print("Requesting funding rates from CoinGlass...")
        # CoinGlass aggregates funding rates from multiple exchanges
        symbols = ['BTC', 'ETH', 'SOL']
        
        for symbol in symbols:
            try:
                response = requests.get(f'https://open-api.coinglass.com/public/v2/funding?symbol={symbol}', timeout=10)
                print(f"CoinGlass response for {symbol}: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and len(data['data']) > 0:
                        # Get the most common exchange (usually Binance)
                        funding_data = data['data'][0]
                        if 'rate' in funding_data:
                            rate = float(funding_data['rate']) * 100
                            funding_rates[symbol] = f"{rate:.4f}%"
                            print(f"Set funding rate for {symbol}: {funding_rates[symbol]}")
                            
            except Exception as symbol_error:
                print(f"Failed to get {symbol} funding rate: {symbol_error}")
                
    except Exception as e:
        print(f"CoinGlass API failed: {e}")
    
    # Fallback: Try OKX API (also free, no auth)
    if all(rate == 'N/A' for rate in funding_rates.values()):
        try:
            print("Trying OKX API fallback...")
            response = requests.get('https://www.okx.com/api/v5/public/funding-rate?instType=SWAP', timeout=10)
            print(f"OKX response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    for item in data['data']:
                        inst_id = item.get('instId', '')
                        if inst_id in ['BTC-USD-SWAP', 'BTC-USDT-SWAP']:
                            rate = float(item['fundingRate']) * 100
                            funding_rates['BTC'] = f"{rate:.4f}%"
                        elif inst_id in ['ETH-USD-SWAP', 'ETH-USDT-SWAP']:
                            rate = float(item['fundingRate']) * 100
                            funding_rates['ETH'] = f"{rate:.4f}%"
                        elif inst_id in ['SOL-USD-SWAP', 'SOL-USDT-SWAP']:
                            rate = float(item['fundingRate']) * 100
                            funding_rates['SOL'] = f"{rate:.4f}%"
                            
        except Exception as okx_error:
            print(f"OKX API failed: {okx_error}")
    
    # Final fallback: Set to 'API Blocked' instead of 'N/A' to indicate the issue
    final_rates = {}
    final_rates['BTCUSDT'] = funding_rates['BTC'] if funding_rates['BTC'] != 'N/A' else 'API Blocked'
    final_rates['ETHUSDT'] = funding_rates['ETH'] if funding_rates['ETH'] != 'N/A' else 'API Blocked' 
    final_rates['SOLUSDT'] = funding_rates['SOL'] if funding_rates['SOL'] != 'N/A' else 'API Blocked'
    
    print(f"Final funding rates: {final_rates}")
    return final_rates

def log_crypto_data():
    """Main function to log crypto data"""
    
    # Get current time in Central Time
    central_tz = pytz.timezone('America/Chicago')
    now_central = datetime.now(central_tz)
    
    print(f"Current Central Time: {now_central.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Check if we should run (3 AM, 11 AM, 7 PM Central)
    current_hour = now_central.hour
    target_hours = [3, 11, 19]  # 3 AM, 11 AM, 7 PM
    
    # For GitHub Actions, we'll run this and let the schedule handle timing
    # But we can still check for safety
    print(f"Current hour: {current_hour}")
    print(f"Target hours: {target_hours}")
    
    # Get all data
    print("Fetching Fear & Greed Index...")
    fear_greed = get_fear_greed_index()
    
    print("Fetching crypto prices...")
    prices = get_crypto_prices()
    
    time.sleep(1)  # Be nice to APIs
    
    print("Fetching funding rates...")
    funding_rates = get_funding_rates()
    
    # Prepare data row
    data_row = {
        'Date': now_central.strftime('%Y-%m-%d'),
        'Time': now_central.strftime('%H:%M:%S'),
        'Fear_Greed_Index': fear_greed,
        'BTC_Price': prices['bitcoin'],
        'BTC_Funding_Rate': funding_rates['BTCUSDT'],
        'BTC_Open_Interest': 'N/A',  # Would need paid API
        'ETH_Price': prices['ethereum'],
        'ETH_Funding_Rate': funding_rates['ETHUSDT'],
        'ETH_Open_Interest': 'N/A',  # Would need paid API
        'SOL_Price': prices['solana'],
        'SOL_Funding_Rate': funding_rates['SOLUSDT'],
        'SOL_Open_Interest': 'N/A'   # Would need paid API
    }
    
    print(f"Data collected: {data_row}")
    
    # Create/append to CSV
    filename = 'crypto_data.csv'
    
    # Check if file exists
    if os.path.exists(filename):
        # Append to existing file
        df_existing = pd.read_csv(filename)
        df_new = pd.DataFrame([data_row])
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        # Create new file
        df_combined = pd.DataFrame([data_row])
    
    # Save to CSV
    df_combined.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    
    # Also save as JSON for GitHub Actions artifact
    with open('latest_data.json', 'w') as f:
        json.dump(data_row, f, indent=2)
    
    return data_row

if __name__ == "__main__":
    try:
        result = log_crypto_data()
        print("Script completed successfully!")
        print(f"Latest data: {result}")
    except Exception as e:
        print(f"Script failed with error: {e}")
        raise  # Re-raise so GitHub Actions sees the failure
