from datetime import datetime
from pandera import Check 
import pandas as pd
import os
from typing import Optional,Tuple, Callable
import logging
from logging import INFO, WARNING, ERROR
import sys
#import pyarrow
import requests
import pandera as pa
sys.path.append('../')
from data_getters.quebec_getters import fetch_data
from data_getters.ontario_getters import fetch_toronto_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/opt/airflow/logs/generic_data_fetcher.log')
    ]
)
logger = logging.getLogger(__name__)

# Mapping from table_name to column order (from 02_import_tables.sql)
TABLE_COLUMNS = {
#'to_fines_buidlings': [ ... ],
    'to_Business': ["_id","AREA_ID","DATE_EFFECTIVE","AREA_ATTR_ID","PARENT_AREA_ID","AREA_SHORT_CODE","AREA_LONG_CODE","AREA_NAME","AREA_DESC","OBJECTID","geometry"],
   }

def fetch_and_store_quebec_dataset(
    resource_id: str,
    output_dir: str = "../master_database/init-scripts/",
    dataset_name: Optional[str] = None,
    limit: Optional[int] = None,
    ssl_verify: bool = False,
    schema: Optional[pa.DataFrameSchema] = None,
    preprocess_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    connection=None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    batch_size: int = 1000,
    **kwargs
) -> Tuple[str, str, int]:
    """
    Fetch, process, and store Québec Open Data datasets in chunks to avoid OOM.
    Writes each batch directly to Postgres if connection and table_name are provided.
    All columns are loaded as text (string) and in the correct order for the raw table.
    All type conversion/cleaning should be done in SQL after loading.
    Returns (parquet_path, csv_path, total_rows_written)
    """
    os.makedirs(output_dir, exist_ok=True)
    execution_date = kwargs.get('execution_date', datetime.now())
    name_safe = dataset_name or f"dataset_{resource_id[:8]}"
    base_filename = f"{name_safe}"
    parquet_path = os.path.join(output_dir, f"{base_filename}.parquet")
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    total_rows = 0
    try:
        logger.info(f"Fetching dataset {resource_id} ({name_safe}) in batches")
        # Get total count
        all_records = []
        for batch in fetch_data(
            resource_id=resource_id,
            limit=limit,
            batch_size=batch_size,
            ssl_verify=ssl_verify
        ):
            if not batch:
                continue
            # Log the structure of batch for debugging
            logger.critical(f"BATCH STRUCTURE: {type(batch)} - {str(batch)[:500]}")
            if not batch:
                df = pd.DataFrame()  # Empty dataset
            elif isinstance(batch, dict):
                logger.info(f"Handling single-record dictionary for {resource_id}")
                df = pd.DataFrame([batch])
            elif isinstance(batch, list) and batch and not isinstance(batch[0], dict):
                logger.warning(f"Scalar list detected for {resource_id}. Creating 'value' column")
                df = pd.DataFrame(batch, columns=["value"])
            else:
                df = pd.DataFrame(batch)
            if preprocess_func:
                logger.info("Applying preprocessing function")
                df = preprocess_func(df)
            if schema:
                logger.info("Validating with schema")
                schema.validate(df)
            # --- Ensure all columns are text and in correct order ---
            if table_name in TABLE_COLUMNS:
                col_order = TABLE_COLUMNS[table_name]
                col_order = [col for col in col_order if col in df.columns]
                df = df[col_order]
            df = df.astype(str)
            if connection and table_name:
                logger.info(f"Writing batch to Postgres: {table_name}")
                df.to_sql(table_name, connection, if_exists='append', index=False, schema=schema_name, method='multi', chunksize=1000)
            df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), encoding='utf-8-sig', index=False)
            total_rows += len(df)
        logger.info(f"Total rows written: {total_rows}")
        return parquet_path, csv_path, total_rows
    except Exception as e:
        logger.error(f"Failed to process {resource_id}: {str(e)}")
        raise


MTL_MAP = {
    'mtl_fines_food': {
        'resource_id': '7f939a08-be8a-45e1-b208-d8744dca8fc6',
        'has_coords': False,
        'category': 'alimentaires'
    },
    'mtl_index_quality_life': {
        'resource_id': '0f0f8c6a-b503-4565-9583-d4f21db9e6fe',
        'has_coords': False
    },
    'mtl_Residential_fines': {
        'resource_id': 'f270cb02-ca30-4b3b-96eb-f0dbdbc50ea7',
        'has_coords': False,
        'description': 'Infractions au Règlement sur la salubrité des logements'
    },
    'mtl_business': {
        'resource_id': 'ceee5da3-56ba-40f4-9008-8678bf4c14fe',
        'has_coords': False,
        'description': 'Fournisseurs de la ville de Montréal'
    },
    'mtl_Residential_properties': {
        'resource_id': '2b9dfc3d-91d3-48de-b32c-a2a6d9417079',
        'has_coords': False
    },
    'mtl_green_transportation': {
        'resource_id': '04e04cd7-96e3-4aed-bc5a-d30dc08c08fc',
        'has_coords': False
    },
    'mtl_financial_help': {
        'resource_id': '5a2bef39-3b3e-4d7d-82cf-658f7821e141',
        'has_coords': False
    },
    'mtl_parking_fines': {
        'resource_id': 'e7df09fb-af5a-476f-861e-ea23777749b5',
        'has_coords': False
    },
    'mtl_ardm': {
        'resource_id': '87af3a62-ee9a-40ad-b7d9-517ab3f12fad',
        'has_coords': False
    },
    'mtl_uadm': {
        'resource_id': '61a4428e-804b-4022-8f1a-8e40c9a46cb1',
        'has_coords': False
    },
    'mtl_Municipalities': {
        'resource_id': '19385b4e-5503-4330-9e59-f998f5918363',
        'has_coords': False
    },
    'mtl_parc': {
        'resource_id': '4731b64f-29cc-4e08-bc44-8752ae2fcafb',
        'has_coords': True
    },
    'mtl_crimes': {
        'resource_id': 'c6f482bf-bf0f-4960-8b2f-9982c211addd',
        'has_coords': True
    },
    'mtl_bus': {
        'resource_id': '59f204f7-b09d-4636-988a-bbe8d3b869b1',
        'has_coords': True
    },
    'mtl_Bugs': {
        'resource_id': 'ba28703e-ce85-4293-8a37-88932bf4ae93',
        'has_coords': True
    },
    'mtl_commercial_sites': {
        'resource_id': 'fb2e534a-c573-45b5-b62b-8f99e3a37cd1',
        'has_coords': True
    },
    'mtl_food_establishments': {
        'resource_id': '28a4957d-732e-48f9-8adb-0624867d9bb0',
        'has_coords': True
    },
    'mtl_fire_fights': {
        'resource_id': '71e86320-e35c-4b4c-878a-e52124294355',
        'has_coords': True
    },
    'mtl_public_sites': {
        'resource_id': '4731b64f-29cc-4e08-bc44-8752ae2fcafb',
        'has_coords': True
    },
    'mtl_retailer_construction_permit': {
        'resource_id': '5232a72d-235a-48eb-ae20-bb9d501300ad',
        'has_coords': True
    },
    'mtl_police_offices': {
        'resource_id': 'c9f296dd-596e-48ed-9c76-37230b2c916d',
        'has_coords': True
    },
    'mtl_Passenger_count': {
        'resource_id': 'f82f00c0-baed-4fa1-8b01-6ed60146d102',
        'has_coords': True
    },
    'mtl_Population': {
        'resource_id': '604cabd1-891d-4327-a85c-5991dccc82d4',
        'has_coords': False
    },
    'mtl_Rich_distribution': {
        'resource_id': '9744f904-6519-4df0-8b0a-8dd4806f619e',
        'has_coords': False
    }
}

def fetch_and_store_ontario_dataset(
    package_id: str,
    output_dir: str = "../master_database/init-scripts/",
    dataset_name: Optional[str] = None,
    limit: Optional[int] = None,
    ssl_verify: bool = False,
    schema: Optional[pa.DataFrameSchema] = None,
    preprocess_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    base_url: str = "https://ckan0.cf.opendata.inter.prod-toronto.ca",
    connection=None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    chunk_size: int = 10000,
    **kwargs
) -> Tuple[str, str, int]:
    """
    Fetch, process, and store Toronto Open Data datasets in chunks to avoid OOM.
    Writes each chunk directly to Postgres if connection and table_name are provided.
    All columns are loaded as text (string) and in the correct order for the raw table.
    All type conversion/cleaning should be done in SQL after loading.
    Returns (csv_path, total_rows_written)
    """
    import numpy as np
    os.makedirs(output_dir, exist_ok=True)
    execution_date = kwargs.get('execution_date', datetime.now())
    name_safe = dataset_name or f"dataset_{package_id[:8]}"
    base_filename = f"{name_safe}"
    parquet_path = os.path.join(output_dir, f"{base_filename}.parquet")
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    total_rows = 0

    try:
        logger.info(f"Fetching dataset {package_id} ({name_safe})")
        from data_getters.ontario_getters import fetch_toronto_data
        df = fetch_toronto_data(
            base_url=base_url,
            package_id=package_id,
            num_observations=limit
        )
        if df.empty:
            logger.info(f"Empty DataFrame {dataset_name} returned from API{package_id}")
            return csv_path, 0
        if preprocess_func:
            logger.info("Applying preprocessing function")
            df = preprocess_func(df)
        if schema:
            logger.info("Validating with schema")
            schema.validate(df)
        # --- Ensure all columns are text and in correct order ---
        if table_name in TABLE_COLUMNS:
            col_order = TABLE_COLUMNS[table_name]
            # Only keep columns that exist in the DataFrame
            col_order = [col for col in col_order if col in df.columns]
            df = df[col_order]
        # Cast all columns to string (text)
        # Convert all columns to string before writing
        df = df.astype(str).replace({"nan": np.nan, "None": np.nan})
        df = df.applymap(lambda x: str(x) if pd.notna(x) else None)
        df.to_csv(csv_path, encoding='utf-8-sig', index=False)
        if connection and table_name:
            logger.info(f"Writing to Postgres in chunks: {table_name}")
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str):
                # Ensure all columns are strings and handle NaN values
                chunk = chunk.applymap(lambda x: str(x) if pd.notna(x) else None)
                chunk.to_sql(table_name, connection, if_exists='append', index=False, schema=schema_name, method='multi', chunksize=1000)
                total_rows += len(chunk)
        else:
            total_rows = len(df)
        logger.info(f"Total rows written: {total_rows}")
        return csv_path, total_rows
    except Exception as e:
        logger.error(f"Failed to process {package_id}: {str(e)}", exc_info=True)
        raise


TO_MAP = {
    'to_neighbourhood': {
        'resource_id': 'neighbourhood-profiles',
        'has_coords': False
    },
     'to_Business': {
        'resource_id': 'business-improvement-areas',
        'has_coords': False
    },
     'to_Business_permits': {
        'resource_id': 'municipal-licensing-and-standards-business-licences-and-permits',
        'has_coords': False
    },
     'to_fines_food': {
        'resource_id': 'dinesafe',
        'has_coords': False
    },
     'to_food_establishments': {
        'resource_id': 'cafeto-curb-lane-parklet-cafe-locations',
        'has_coords': False
    },
     'to_transportation_cycle': {
        'resource_id': 'bicycle-parking-racks',
        'has_coords': False
    },
     'to_crimes': {
        'resource_id': 'neighbourhood-crime-rates',
        'has_coords': False
    },
     'to_short_rentals': {
        'resource_id': 'short-term-rentals-registration',
        'has_coords': False
    },
     'to_sites_opportunities': {
        'resource_id': 'toronto-signature-sites',
        'has_coords': False
    },
     'to_permits': {
        'resource_id': 'building-permits-cleared-permits',
        'has_coords': False
    },
     'to_fines_buidlings': {
        'resource_id': 'apartment-building-evaluation',
        'has_coords': False
    },
     'to_public_solar_pannels': {
        'resource_id': 'renewable-energy-installations',
        'has_coords': False
    },
     'to_private_solar_pannels': {
        'resource_id': 'solarto',
        'has_coords': False
    },
     'to_Bids_Solicitations': {
        'resource_id': 'tobids-all-open-solicitations',
        'has_coords': False
    },
     'to_barbers_inspections': {
        'resource_id': 'bodysafe',
        'has_coords': False
    },
     'to_residential_properties': {
        'resource_id': 'apartment-building-registration',
        'has_coords': False
    },
     'to_health_casualties': {
        'resource_id': 'wellbeing-toronto-health',
        'has_coords': False
    },
     'to_ward_profiling': {
        'resource_id': 'ward-profiles-25-ward-model',
        'has_coords': False
    },
     'to_health_outbreaks': {
        'resource_id': 'outbreaks-in-toronto-healthcare-institutions',
        'has_coords': False
    },
     'to_labor_force': {
        'resource_id': 'labour-force-survey',
        'has_coords': False
    },
     'to_Building_permits_signs': {
        'resource_id': 'building-permits-signs',
        'has_coords': False
    },
     'to_Street_tree': {
        'resource_id': 'street-tree-data',
        'has_coords': False
    },
     'to_KPIS': {
        'resource_id': 'toronto-s-dashboard-key-indicators',
        'has_coords': False
    },
     'to_cycle_parking': {
        'resource_id': 'bicycle-parking-bike-stations-indoor',
        'has_coords': False
    },
     'to_neighjbourhood_housing_density': {
        'resource_id': 'social-housing-unit-density-by-neighbourhoods',
        'has_coords': False
    },
     'to_employment': {
        'resource_id': 'toronto-employment-survey-summary-tables',
        'has_coords': False
    },
     'to_real_estate_inventory': {
        'resource_id': 'real-estate-asset-inventory',
        'has_coords': False
    },
     'to_parking_fines': {
        'resource_id': 'parking-tickets',
        'has_coords': True
    },
     'to_building_active_permits': {
        'resource_id': 'building-permits-active-permits',
        'has_coords': False
    },
     'to_fire_fights': {
        'resource_id': 'fire-incidents',
        'has_coords': False
    },
     'to_parking_lots_facilities': {
        'resource_id': 'parking-lot-facilities',
        'has_coords': True
    },
     'to_noisy_places': {
        'resource_id': 'noise-exemption-permits',
        'has_coords': False
    },
     'to_wards': {
        'resource_id': 'city-wards',
        'has_coords': False
    },
     'to_on_street_permit_parking_area_maps': {
        'resource_id': 'on-street-permit-parking-area-maps',
        'has_coords': False
    },
     'to_property_boundaries': {
        'resource_id': 'property-boundaries',
        'has_coords': False
    },
     'to_green_energy_installations': {
        'resource_id': 'renewable-energy-installations',
        'has_coords': False
    },
     'to_non_competitive_contracts': {
        'resource_id': 'tobids-non-competitive-contracts',
        'has_coords': False
    },
     'to_fire_inspections': {
        'resource_id': 'highrise-residential-fire-inspection-results',
        'has_coords': False
    },
     'to_green_roofs': {
        'resource_id': 'building-permits-green-roofs',
        'has_coords': False
    },
     'to_mappings': {
        'resource_id': 'web-map-services',
        'has_coords': False
    },
     'to_street_routes': {
        'resource_id': 'ttc-routes-and-schedules',
        'has_coords': False
    },
     'to_apt_registrations': {
        'resource_id': 'apartment-building-registration',
        'has_coords': False
    }
}