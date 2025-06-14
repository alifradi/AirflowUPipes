-- Toronto Bids
DROP TABLE IF EXISTS public_transformed_data.tf_to_bids_solicitations CASCADE;
CREATE TABLE public_transformed_data.tf_to_bids_solicitations AS
SELECT
"Issue Date"::DATE AS issue_date,
"Submission Deadline"::DATE AS submission_deadline,
"Division",
"NOIP (Notice of Intended Procurement) Type" AS procurement_type,
"High Level Category" AS category,
"Solicitation Document Description" AS description,
"Buyer Name","Buyer Email","Buyer Phone Number",
'COMPETITIVE CONTRACTS BIDS' AS data_category,
'https://open.toronto.ca/dataset/tobids-all-open-solicitations/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_bids_solicitations;

-- Toronto Food Establishments
SELECT
CONCAT(LOWER("MUNICIPAL_ADDRESS"), ', ', "WARD_NAME", ', ', LOWER("BUSINESS_ADDRESS"), ' Toronto, ON, Canada') AS address,
"OPERATOR_NAME" AS business_name,
LOWER("CATEGORY") AS category,
CASE
	WHEN "OPERATOR_NAME" IS NULL THEN 'Closed'
	ELSE 'Open'
END AS status,
NULL::DATE AS status_update_date,
("geometry"::jsonb->'coordinates'->>1)::REAL AS latitude,
("geometry"::jsonb->'coordinates'->>0)::REAL AS longitude,
"INTERVENTION_TYPE",
'https://open.toronto.ca/dataset/cafeto-curb-lane-parklet-cafe-locations/' AS data_source,
CURRENT_DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_food_establishments;

-- Toronto Food Fines
SELECT DISTINCT
"Establishment Name" AS business_name,
CONCAT(LOWER("Establishment Address"), ', Toronto, ON, Canada') AS address,
"Establishment Type" AS category,
"Inspection Date"::DATE AS inspection_date,
"Establishment Status" AS inspection_outcome,
"Action" AS measure_taken,
"Min. Inspections Per Year" AS min_inspec_per_year,
"Infraction Details" AS infraction_details,
"Latitude"::REAL AS latitude,
"Longitude"::REAL AS longitude,
'https://open.toronto.ca/dataset/dinesafe/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO'::TEXT AS city
FROM ontario_data.to_fines_food;

-- Business Permits
SELECT
CONCAT(LOWER("Licence Address Line 1"), ', ', LOWER("Licence Address Line 3"), ', ',LOWER("Licence Address Line 2"), ' Canada') AS address,
"Client Name" AS business_name,
LOWER("Category") AS category,
"Endorsements" AS permit_activitiy,
"Issued"::DATE AS issued_date,
"Cancel Date" AS cancel_date,
"Last Record Update"::DATE AS last_update_date,
'https://open.toronto.ca/dataset/municipal-licensing-and-standards-business-licences-and-permits/' AS data_source,
CURRENT_DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_business_permits;

-- Non-Competitive Contracts
SELECT
"Contract Date"::DATE AS contract_date,
"Contract Amount"::TEXT AS contract_amount,
"Reason",
"Division",
"Supplier Name" AS supplier,
"Supplier Address" AS supplier_address,
'https://open.toronto.ca/dataset/tobids-non-competitive-contracts/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY'  AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_non_competitive_contracts;

-- Building Permits (Active)
SELECT
CONCAT("STREET_NUM",', ', "STREET_TYPE",', ',LOWER("STREET_NAME"),', ',"WARD_GRID",', ',"POSTAL",', Toronto, ON, Canada') AS address,
"APPLICATION_DATE"::DATE AS request_permit_date,
"ISSUED_DATE"::TEXT AS issued_date,
"PERMIT_TYPE" AS category,
REPLACE("DWELLING_UNITS_CREATED",',','')::REAL AS dwelling_units_created,
REPLACE("DWELLING_UNITS_LOST",',','')::REAL AS dwelling_units_lost,
CONCAT("STRUCTURE_TYPE",', ', "WORK",', ',"DESCRIPTION") AS commentaries,
"STATUS",
REPLACE(CASE WHEN "EST_CONST_COST" ='DO NOT UPDATE OR DELETE THIS INFO FIELD' THEN NULL ELSE "EST_CONST_COST" END,',','')::REAL AS cost_cad,
'https://open.toronto.ca/dataset/building-permits-active-permits/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_building_active_permits;

-- Sign Permits
SELECT
CONCAT("STREET_NUMBER",', ', "STREET_TYPE",', ',"STREET_NAME",', ',"POSTAL",', Toronto, ON, Canada') AS address,
"APPLICATION_DATE"::DATE AS application_date,
"ISSUED_DATE"::TEXT AS issued_date,
"TYPE" AS category,
CONCAT('Use from: ',"CURRENT_USE",' :to: ',"PROPOSED_USE") AS use_transition,
"STATUS",
'https://open.toronto.ca/dataset/building-permits-signs/' AS data_source,
CURRENT_DATE AS data_pull_date,
'WEEKLY'  AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_building_permits_signs;

-- Noise Permits
SELECT
"Issue Date"::TEXT AS issued_date,
"Permit Type" AS category,
"Expected End Date"::DATE AS end_date_expected,
"Actual End Date"::DATE AS end_date_effective,
CONCAT("Operating Name",'& ',"Client Name") AS contractor,
'https://open.toronto.ca/dataset/noise-exemption-permits/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY'  AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_noisy_places;

-- Green Energy
SELECT
CONCAT("ADDRESS_FULL",', ',"POSTAL_CODE",', ',"MUNICIPALITY",', ',"WARD",', ',"CITY",', ',"PLACE_NAME") AS address,
"BUILDING_NAME",
CASE
  WHEN TRIM("YEAR_INSTALL") ~ '^\d{4}$' THEN TRIM("YEAR_INSTALL") 
  WHEN TRIM("YEAR_INSTALL") ~ '^[A-Za-z]+\s+\d{1,2},\s+\d{4}$' THEN substring(TRIM("YEAR_INSTALL") from '\d{4}$')
  END::NUMERIC AS year_install,
("geometry"::jsonb->'coordinates'->>1)::REAL AS latitude,
("geometry"::jsonb->'coordinates'->>0)::REAL AS longitude,
"TYPE_INSTALL",
"SIZE_INSTALL"::REAL AS size_install,
"GENERAL_USE" AS electricity_consumption,
'https://open.toronto.ca/dataset/renewable-energy-installations/' AS data_source,
CURRENT_DATE AS data_pull_date,
'UNKNOWN'::VARCHAR AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_green_energy_installations;

-- Real Estate
SELECT
CONCAT("Address", ', ', "Ward Name", ', ', "City", ', Canada') AS address,
("geometry"::jsonb->'coordinates'->1->0->>0)::REAL AS latitude,
("geometry"::jsonb->'coordinates'->0->0->>0)::REAL AS longitude,
"Building Status" AS status,
"Management" AS usage,
"Gross Floor Area (M2)"::REAL AS building_area_sqm,
"Gross Floor Area (FT2)"::REAL AS building_area_ft,
REPLACE("Year Built",'.0','')::INTEGER AS construction_year,
"Owner" AS contractor,
'https://open.toronto.ca/dataset/real-estate-asset-inventory/' AS data_source,
CURRENT_DATE AS data_pull_date,
'WEEKLY'  AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_real_estate_inventory;

-- Building Evaluations
SELECT
CONCAT("SITE_ADDRESS", ', ', "WARDNAME", ', ', 'Toronto, ON, Canada') AS address,
"EVALUATION_COMPLETED_ON"::DATE AS review_date,
"CONFIRMED_STOREYS"::REAL AS  floors_count,
"CONFIRMED_UNITS"::REAL AS unit_count,
"NO_OF_AREAS_EVALUATED",  
REPLACE("YEAR_BUILT",'.0','')::INTEGER AS year_built,
REPLACE("YEAR_REGISTERED",'.0','')::INTEGER AS year_registered,
"PROPERTY_TYPE",
"SCORE"::REAL AS score,
"LATITUDE"::REAL AS latitude,
"LONGITUDE"::REAL AS longitude,
'https://open.toronto.ca/dataset/apartment-building-evaluation/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_fines_buidlings;

-- Fire Incidents
SELECT
"Intersection" AS address,
"TFS_Alarm_Time"::TEXT AS alarm_time,
"Latitude"::REAL AS latitude,
"Longitude"::REAL AS longitude,
COALESCE("Civilian_Casualties"::REAL,0)+COALESCE("TFS_Firefighter_Casualties"::REAL,0)::REAL AS casualties,
COALESCE("Count_of_Persons_Rescued"::REAL,0)+COALESCE("Estimated_Number_Of_Persons_Displaced"::REAL,0)::REAL AS rescued,
"Estimated_Dollar_Loss"::REAL AS estimated_loss,
ROUND(EXTRACT(EPOCH FROM ("Last_TFS_Unit_Clear_Time"::TIMESTAMP - "TFS_Arrival_Time"::TIMESTAMP)) / 60, 2) AS duration_minutes,
'https://open.toronto.ca/dataset/fire-incidents/' AS data_source,
CURRENT_DATE AS data_pull_date,
'ANNUALY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_fire_fights;

-- Neighborhood Crimes
SELECT
CONCAT("AREA_NAME",', Toronto, ON, Canada')::TEXT AS address,
"year"::INTEGER AS incident_datetime,
(SELECT AVG((point->>1)::NUMERIC) FROM jsonb_array_elements("geometry"::jsonb->'coordinates'->0) AS point )::REAL AS latitude,
(SELECT AVG((point->>0)::NUMERIC) FROM jsonb_array_elements("geometry"::jsonb->'coordinates'->0) AS point )::REAL AS longitude,
"crime" AS incident_type,
CONCAT('Count: ', "count", ', Rate: ', "rate") AS details,
'https://open.toronto.ca/dataset/neighbourhood-crime-rates/' AS data_source,
CURRENT_DATE AS data_pull_date,
'ANNUALY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_crimes;

-- Barber Inspections
SELECT
CONCAT(LOWER("addrFull"),', Tonronto, ON, Canada') AS address,
"estName" AS establishment_name,
"srvType" AS category,
("geometry"::jsonb->'coordinates'->>1)::REAL AS latitude,
("geometry"::jsonb->'coordinates'->>0)::REAL AS longitude,
coalesce("observation", "infCategory") AS violation_type,
CONCAT('Infraction Level: ', "infType") AS details,
"insStatus" AS status,
"insDate"::DATE AS status_date,
"actionDesc" AS actions,
'https://open.toronto.ca/dataset/bodysafe/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_barbers_inspections;

-- Health Outbreaks
SELECT
CONCAT(TRIM("Institution Address"),', ','Toronto, ON, Canada') AS address,
"Institution Name" AS establishment_name,
"Date Outbreak Began"::DATE AS inspection_datetime,
COALESCE("Causative Agent-1","Causative Agent-2") AS violation_type,
CASE WHEN "Active" ='Y' THEN 'Active' ELSE 'Inactive' END::TEXT AS status,
"Date Outbreak Began"::DATE AS status_date,
'https://open.toronto.ca/dataset/outbreaks-in-toronto-healthcare-institutions/' AS data_source,
CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_health_outbreaks;

-- Fire Inspections
SELECT
CONCAT("PropertyAddress",', ',"propertyWard",', Toronto, ON, Canada') AS address,
"INSPECTIONS_OPENDATE"::DATE AS inspection_started,
"INSPECTIONS_CLOSEDDATE"::DATE AS inspection_ended,
"VIOLATION_DESCRIPTION" AS violation_type,
REPLACE("VIOLATIONS_ITEM_NUMBER",'.0',''):: INTEGER AS violations_count,
'https://open.toronto.ca/dataset/highrise-residential-fire-inspection-results/' AS data_source,
 CURRENT_DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_fire_inspections;

-- Development Sites
SELECT
"site_name",
"owner" AS owner_name,
CONCAT("site_name",', ', "district",' Toronto, ON, Canada') AS address,
("geometry"::jsonb->'coordinates'->>1)::REAL AS latitude,
("geometry"::jsonb->'coordinates'->>0)::REAL AS longitude,
CASE
	WHEN "statement" ILIKE '%sale%' THEN 'sale'
	WHEN "statement" ILIKE '%lease%' THEN 'lease'
	ELSE 'other'
END AS open_for,
"statement",
"status",
CONCAT("property_type_primary",'| ',"property_type_secondary") AS category,
"planning_status",
    -- Extract numeric value from total_site_area_acres (handling units/text)
    CASE
        WHEN "total_site_area_acres" ~ '\d' THEN 
            REGEXP_REPLACE("total_site_area_acres", '[^\d.]', '', 'g') 
        ELSE NULL
    END AS total_site_area_acres_numeric,

    -- Standardize site_area_range_1 into interval format (e.g., "0 - 5" → "0-5")
    CASE
        WHEN "site_area_range_1" ~ '\d' THEN REPLACE("site_area_range_1", ' ', '')
        ELSE NULL
    END AS site_area_range_1_interval,

    -- Extract numeric value from frontage_feet (first occurrence)
    CASE
        WHEN "frontage_feet" ~ '\d' THEN 
            REGEXP_REPLACE(SUBSTRING("frontage_feet" FROM '(\d+\.?\d*)'), '[^\d.]', '', 'g') 
        ELSE NULL
    END AS frontage_feet_numeric,

    -- Convert remaining columns to intervals or text (ensuring consistent output type)
    CASE
        WHEN "site_area_range2" ~ '\d' THEN REPLACE("site_area_range2", ' ', '')
        ELSE NULL
    END AS site_area_range2_interval,

    CASE
        WHEN "sq_ft_available" ~ '\d' THEN 
            CASE
                -- Return ranges as text (e.g., "100001-300000")
                WHEN "sq_ft_available" ~ '-' THEN REPLACE("sq_ft_available", ' ', '')
                -- Return numbers as text (to match the CASE type)
                ELSE REGEXP_REPLACE("sq_ft_available", '[^\d.]', '', 'g')
            END
        ELSE NULL
    END AS sq_ft_available_clean,

    -- Predefined range columns (clean formatting)
    REPLACE("sq_ft_available_range1", ' ', '') AS sq_ft_available_range1_interval,
    REPLACE("sq_ft_available_range2", ' ', '') AS sq_ft_available_range2_interval,

CONCAT("phone3",', ', "email3") AS person3,
CONCAT("company2",', ',"phone2",', ', "email2") AS person2,
CONCAT("company1",', ', "phone1",', ',"email1") AS person1,
"comments",
'https://open.toronto.ca/dataset/toronto-signature-sites/' AS data_source,
CURRENT_DATE AS data_pull_date,
'MONTHLY'  AS data_refresh,
'TORONTO' AS city
FROM ontario_data.to_sites_opportunities;

-- Cycle Parking
SELECT 
  CONCAT(
    COALESCE("ADDRESS_FULL", ''), ', ',
    COALESCE("POSTAL_CODE", ''), ', ',
    COALESCE("WARD_NAME", ''), ', ',
    COALESCE("CITY", ''), ', Canada'
  ) AS address,
  "BIKE_CAPACITY"::INTEGER AS bike_capacity,
  "CHANGE_ROOM" AS change_room,
  "VENDING_MACHINE" AS vending_machine,
  "TOOLS_PUMP" AS tools_pump,
  NULLIF("YR_INSTALL", 'None')::INTEGER AS yr_install,
  (NULLIF(geometry,'None')::jsonb->'coordinates'->>1)::REAL AS latitude,
  (NULLIF(geometry,'None')::jsonb->'coordinates'->>0)::REAL AS longitude,
  'https://open.toronto.ca/dataset/bicycle-parking-bike-stations-indoor/' AS data_source,
  CURRENT_DATE AS data_pull_date,
  'DAILY' AS data_refresh,
  'TORONTO' AS city
FROM ontario_data.to_cycle_parking;

-- Apartment Registrations (atomic stepwise transformation)

-- Step 1: Create temporary staging table
CREATE TEMP TABLE staging_apt_reg AS
SELECT 
  *,
  ROW_NUMBER() OVER () AS row_id
FROM ontario_data.to_apt_registrations;

-- Step 2: Create unpivoted data table
CREATE TEMP TABLE unpivoted_apt_reg AS
SELECT
  row_id,
  CONCAT(TRIM("SITE_ADDRESS"), ', Toronto, ON, Canada') AS address,
  "PROPERTY_TYPE" AS category,
  "NO_OF_STOREYS"::INTEGER AS floors_count,
  "NO_OF_UNITS"::INTEGER AS unit_counts,
  "PROP_MANAGEMENT_COMPANY_NAME" AS management_company,
  CASE WHEN "YEAR_REGISTERED" ~ '^\d+$' THEN "YEAR_REGISTERED"::INTEGER ELSE NULL END AS year_registered,
  CASE WHEN "YEAR_OF_REPLACEMENT" ~ '^\d+$' THEN "YEAR_OF_REPLACEMENT"::INTEGER ELSE NULL END AS year_of_replacement,
  CASE WHEN "YEAR_BUILT" ~ '^\d+$' THEN "YEAR_BUILT"::INTEGER ELSE NULL END AS year_built,
  "DATE_OF_LAST_INSPECTION_BY_TSSA"::DATE AS last_inspection_date,
  building_part,
  attribute,
  'https://open.toronto.ca/dataset/apartment-building-registration/' AS data_source,
    CURRENT_DATE AS data_pull_date,
  'MONTHLY' AS data_refresh,
  'TORONTO' AS city
FROM staging_apt_reg
CROSS JOIN LATERAL (
  VALUES
    ('air_conditioning_type', "AIR_CONDITIONING_TYPE"::TEXT),
    ('amenities_available', "AMENITIES_AVAILABLE"::TEXT),
    ('annual_fire_alarm_test_records', "ANNUAL_FIRE_ALARM_TEST_RECORDS"::TEXT),
    ('annual_fire_pump_flow_test_records', "ANNUAL_FIRE_PUMP_FLOW_TEST_RECORDS"::TEXT),
    ('approved_fire_safety_plan', "APPROVED_FIRE_SAFETY_PLAN"::TEXT),
    ('balconies', "BALCONIES"::TEXT),
    ('barrier_free_accessibility_entr', "BARRIER_FREE_ACCESSIBILTY_ENTR"::TEXT),
    ('bike_parking', "BIKE_PARKING"::TEXT),
    ('description_of_child_play_area', "DESCRIPTION_OF_CHILD_PLAY_AREA"::TEXT),
    ('description_of_indoor_exercise_room', "DESCRIPTION_OF_INDOOR_EXERCISE_ROOM"::TEXT),
    ('description_of_outdoor_rec_facilities', "DESCRIPTION_OF_OUTDOOR_REC_FACILITIES"::TEXT),
    ('elevator_parts_replaced', "ELEVATOR_PARTS_REPLACED"::TEXT),
    ('elevator_status', "ELEVATOR_STATUS"::TEXT),
    ('emerg_power_supply_test_records', "EMERG_POWER_SUPPLY_TEST_RECORDS"::TEXT),
    ('exterior_fire_escape', "EXTERIOR_FIRE_ESCAPE"::TEXT),
    ('facilities_available', "FACILITIES_AVAILABLE?"::TEXT),
    ('fire_alarm', "FIRE_ALARM"::TEXT),
    ('garbage_chutes', "GARBAGE_CHUTES"::TEXT),
    ('green_bin_location', "GREEN_BIN_LOCATION"::TEXT),
    ('heating_equipment_status', "HEATING_EQUIPMENT_STATUS"::TEXT),
    ('heating_equipment_year_installed', "HEATING_EQUIPMENT_YEAR_INSTALLED"::TEXT),
    ('heating_type', "HEATING_TYPE"::TEXT),
    ('indoor_garbage_storage_area', "INDOOR_GARBAGE_STORAGE_AREA"::TEXT),
    ('intercom', "INTERCOM"::TEXT),
    ('is_there_a_cooling_room', "IS_THERE_A_COOLING_ROOM?"::TEXT),
    ('is_there_emergency_power', "IS_THERE_EMERGENCY_POWER?"::TEXT),
    ('laundry_room', "LAUNDRY_ROOM"::TEXT),
    ('laundry_room_hours_of_operation', "LAUNDRY_ROOM_HOURS_OF_OPERATION"::TEXT),
    ('laundry_room_location', "LAUNDRY_ROOM_LOCATION"::TEXT),
    ('locker_or_storage_room', "LOCKER_OR_STORAGE_ROOM"::TEXT),
    ('no_barrier_free_accessible_units', "NO_BARRIERFREE_ACCESSBLE_UNITS"::TEXT),
    ('no_of_accessible_parking_spaces', "NO_OF_ACCESSIBLEPARKING_SPACES"::TEXT),
    ('no_of_elevators', "NO_OF_ELEVATORS"::TEXT),
    ('no_of_laundry_room_machines', "NO_OF_LAUNDRY_ROOM_MACHINES"::TEXT),
    ('non_smoking_building', "NON-SMOKING_BUILDING"::TEXT),
    ('outdoor_garbage_storage_area', "OUTDOOR_GARBAGE_STORAGE_AREA"::TEXT),
    ('parking_type', "PARKING_TYPE"::TEXT),
    ('pet_restrictions', "PET_RESTRICTIONS"::TEXT),
    ('pets_allowed', "PETS_ALLOWED"::TEXT),
    ('recycling_bins_location', "RECYCLING_BINS_LOCATION"::TEXT),
    ('separate_gas_meters_each_unit', "SEPARATE_GAS_METERS_EACH_UNIT"::TEXT),
    ('separate_hydro_meter_each_unit', "SEPARATE_HYDRO_METER_EACH_UNIT"::TEXT),
    ('separate_water_meters_ea_unit', "SEPARATE_WATER_METERS_EA_UNIT"::TEXT),
    ('sprinkler_system', "SPRINKLER_SYSTEM"::TEXT)
) AS unpivoted(building_part, attribute);

-- Step 3: Create final transformed table
CREATE TABLE public_transformed_data.tf_to_apt_registrations AS
SELECT
  address,
  CONCAT(address, ', ', building_part) AS amenity,
  category,
  floors_count,
  management_company,
  year_registered,
  year_of_replacement,
  year_built,
  last_inspection_date,
  building_part,
  attribute,
  data_source,
  CURRENT_DATE AS data_pull_date,
  data_refresh,
  city
FROM unpivoted_apt_reg;

-- Step 4: Cleanup temporary tables
DROP TABLE IF EXISTS staging_apt_reg;
DROP TABLE IF EXISTS unpivoted_apt_reg;
