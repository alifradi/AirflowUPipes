# dags/common/geocoding_utils.py
import pandas as pd
import geopy #
from geopy.geocoders import Nominatim, ArcGIS #
from geopy.extra.rate_limiter import RateLimiter #
import concurrent.futures #
# Removed threading, pickle, os (for cache path), hashlib as they were primarily for the .pkl cache
# os is still used for AIRFLOW_HOME if other configurations need it, but not for geocoding cache here.
import logging
import random # For unique user-agent

# tqdm is useful for command-line scripts but less so in Airflow logs.
# You can remove it or use conditional import if preferred.
from tqdm import tqdm #

logger = logging.getLogger(__name__)

class OptimizedGeocoder:
    def __init__(self, use_multiple_services=True): # Removed cache_file argument
        self.geocoders = []
        if use_multiple_services:
            try:
                # Using a slightly randomized user-agent for Nominatim
                nominatim_agent = f"airflow_pipeline_geocoder_v1_{random.randint(1000,9999)}"
                nominatim = Nominatim(user_agent=nominatim_agent, timeout=10) #
                self.geocoders.append(("Nominatim", RateLimiter(nominatim.geocode, min_delay_seconds=1.1))) # Respect Nominatim's policy
                
                arcgis = ArcGIS(timeout=10) #
                self.geocoders.append(("ArcGIS", RateLimiter(arcgis.geocode, min_delay_seconds=0.5))) #
            except Exception as e:
                logger.warning(f"Warning: Error initializing geocoders: {e}")
                if not self.geocoders: # Fallback if all initializations failed
                    nominatim_agent = f"airflow_pipeline_geocoder_fallback_{random.randint(1000,9999)}"
                    nominatim = Nominatim(user_agent=nominatim_agent, timeout=10)
                    self.geocoders = [("Nominatim", RateLimiter(nominatim.geocode, min_delay_seconds=1.1))]
        else: # Fallback to basic Nominatim if multiple services are disabled
            nominatim_agent = f"airflow_pipeline_geocoder_single_{random.randint(1000,9999)}"
            nominatim = Nominatim(user_agent=nominatim_agent, timeout=10)
            self.geocoders = [("Nominatim", RateLimiter(nominatim.geocode, min_delay_seconds=1.1))]

    # Removed load_cache(), save_cache(), self.cache, self.cache_lock, self.cache_file, get_cache_key()

    def normalize_address(self, address):
        if not address or pd.isna(address):
            return ""
        normalized = str(address).lower().strip() #
        # Add Montreal, Quebec context if not present
        if "montreal" not in normalized and "québec" not in normalized and "quebec" not in normalized:
            normalized += ", montreal, quebec, canada"
        elif "quebec" not in normalized and "québec" not in normalized:
            normalized += ", quebec, canada"
        elif "canada" not in normalized:
            normalized += ", canada"
        return normalized

    def geocode_single(self, address):
        if not address or pd.isna(address):
            return (None, None)
        
        # No file-based cache check or saving
        normalized_address = self.normalize_address(address)
        
        for service_name, geocoder_func in self.geocoders:
            try:
                location = geocoder_func(normalized_address) #
                if location:
                    return (location.latitude, location.longitude) #
            except Exception as e:
                logger.warning(f"Warning: {service_name} failed for address '{address}' (normalized: '{normalized_address}'): {str(e)}")
                continue # Try next geocoding service
        
        return (None, None) # Return None if all services fail

def preprocess_addresses(df, address_column): #
    logger.info("Preprocessing addresses...")
    if address_column not in df.columns:
        logger.error(f"Address column '{address_column}' not found in DataFrame.")
        return pd.Series(dtype='str')
    
    unique_addresses = df[address_column].dropna().unique() #
    logger.info(f"Found {len(unique_addresses)} unique addresses out of {len(df[address_column].dropna())} non-null addresses.")
    return unique_addresses

def geocode_batch_parallel(addresses, max_workers=5, batch_size=50): #
    if not isinstance(addresses, list):
        addresses = list(addresses)

    if not addresses:
        logger.info("No addresses to geocode.")
        return {}

    geocoder_instance = OptimizedGeocoder() # This instance will not use file-caching
    results = {}

    def geocode_address_wrapper(address):
        return address, geocoder_instance.geocode_single(address)

    logger.info(f"Processing {len(addresses)} addresses in batches using up to {max_workers} workers (no file cache).")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor: #
        num_batches = (len(addresses) + batch_size - 1) // batch_size
        for i in range(num_batches):
            batch_addresses = addresses[i*batch_size : (i+1)*batch_size]
            futures = [executor.submit(geocode_address_wrapper, addr) for addr in batch_addresses] #
            for future in concurrent.futures.as_completed(futures): #
                address, coords = future.result() #
                results[address] = coords
            logger.info(f"Completed geocoding batch {i+1}/{num_batches}")
    
    # No geocoder_instance.save_cache() call needed
    return results

def apply_coordinates_to_dataframe(df, address_column, coordinate_results): #
    logger.info("Applying coordinates to dataframe...")
    
    if address_column not in df.columns:
        logger.error(f"Address column '{address_column}' not found for applying coordinates.")
        df['latitude'] = None
        df['longitude'] = None
        return df

    def get_coordinates(address):
        return coordinate_results.get(address, (None, None))

    coords_list = []
    # Can use df[address_column].progress_apply(get_coordinates) if tqdm is desired
    for address in df[address_column]: #
        coords_list.append(get_coordinates(address))
    
    df[['latitude', 'longitude']] = pd.DataFrame(coords_list, index=df.index) #
    
    return df