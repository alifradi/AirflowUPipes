import requests
import pandas as pd
import io

def fetch_toronto_data(base_url, package_id, num_observations=None):
    # Fetch package information
    url = base_url + "/api/3/action/package_show"
    params = {"id": package_id}
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise exception for HTTP errors
    package = response.json()

    data_list = []
    total_collected = 0

    # Process each resource
    for resource in package["result"]["resources"]:
        if not resource["datastore_active"]:
            continue

        # Exit early if we've collected enough data
        if num_observations is not None and total_collected >= num_observations:
            break

        # Handle limited observations case
        if num_observations is not None:
            remaining = num_observations - total_collected
            if remaining <= 0:
                break

            # Fetch limited data using datastore_search
            search_url = base_url + "/api/3/action/datastore_search"
            params = {
                "resource_id": resource["id"],
                "limit": remaining
            }
            resource_response = requests.get(search_url, params=params)
            
            if resource_response.status_code != 200:
                print(f"Error fetching resource {resource['id']}: HTTP {resource_response.status_code}")
                continue
            
            resource_data = resource_response.json()
            if not resource_data.get("success", False):
                print(f"API error for resource {resource['id']}: {resource_data.get('error', 'Unknown error')}")
                continue

            records = resource_data.get("result", {}).get("records", [])
            if records:
                df = pd.DataFrame(records)
                data_list.append(df)
                total_collected += len(df)

        else:
            # Fetch all data using the dump method
            dump_url = base_url + "/datastore/dump/" + resource["id"]
            dump_response = requests.get(dump_url)
            
            if dump_response.status_code != 200:
                print(f"Error fetching dump for resource {resource['id']}: HTTP {dump_response.status_code}")
                continue

            try:
                df = pd.read_csv(io.StringIO(dump_response.text), on_bad_lines='skip')
                data_list.append(df)
            except pd.errors.ParserError as e:
                print(f"Error parsing CSV for resource {resource['id']}: {e}")

    # Combine all data and truncate if necessary
    df = pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()
    
    if num_observations is not None and len(df) > num_observations:
        df = df.head(num_observations)

    print(f"Final dataset shape: {df.shape}")
    return df
