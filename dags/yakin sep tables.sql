SELECT
CONCAT(nom_arrond, ', ', nom_ville, ', Montreal, QC, Canada') AS address,
creation_date_time::TIMESTAMP AS incident_datetime,
incident_type_desc AS description,
description_groupe AS category,
caserne::NUMERIC AS caserne,
division::NUMERIC AS division,
nombre_unites::NUMERIC AS unit_count,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-interventions-service-securite-incendie-montreal/resource/71e86320-e35c-4b4c-878a-e52124294355' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'WEEKLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_fire_fights

SELECT
CONCAT(date, ' ', periode)::TIMESTAMP AS record_datetime,
description_code_banque AS location_description,
(NBLT::NUMERIC + NBT::NUMERIC + NBRT::NUMERIC +
SBLT::NUMERIC + SBT::NUMERIC + SBRT::NUMERIC +
EBLT::NUMERIC + EBT::NUMERIC + EBRT::NUMERIC +
WBLT::NUMERIC + WBT::NUMERIC + WBRT::NUMERIC +
Approche_Nord::NUMERIC + Approche_Sud::NUMERIC +
Approche_Est::NUMERIC + Approche_Ouest::NUMERIC)::NUMERIC AS total_passengers,
longitude::REAL,
latitude::REAL,    
'https://www.donneesquebec.ca/recherche/dataset/vmtl-comptage-vehicules-pietons/resource/f82f00c0-baed-4fa1-8b01-6ed60146d102' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL'::TEXT AS city
FROM mtl_passenger_count

SELECT
"Issue Date"::DATE AS issue_date,
"Submission Deadline"::DATE AS submission_deadline,
division,
"NOIP (Notice of Intended Procurement) Type" AS Procurement_type,
"High Level Category" AS category,
"Solicitation Document Description" AS description,
division,
"Buyer Name","Buyer Email","Buyer Phone Number",
'COMPETITIVE CONTRACTS BIDS' AS data_category,
'https://open.toronto.ca/dataset/tobids-all-open-solicitations/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM to_bids_solicitations

SELECT DISTINCT
etablissement AS business_name,
TRIM(adresse) AS address,
categorie AS category,
proprietaire AS owner_name,
statut AS status,
"date"::DATE AS infraction_date,
date_statut::DATE AS status_update_date,
date_jugement::DATE AS date_jugement,
description,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-inspection-aliments-contrevenants/resource/7f939a08-be8a-45e1-b208-d8744dca8fc6' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL'::TEXT AS city
FROM mtl_fines_food


SELECT
CONCAT(TRIM(address),', ',city,', ',"state") AS address,
"name" AS business_name,
"type" AS category,
statut AS status,
date_statut::DATE AS status_update_date,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-etablissements-alimentaires/resource/28a4957d-732e-48f9-8adb-0624867d9bb0' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL'::TEXT AS city
FROM mtl_food_establishments


SELECT
date_creation::DATE AS creation_date,
nom_etab AS business_name,
CASE
	WHEN vacant_a_louer = 'Non' THEN 'No'
	ELSE 'Yes'
END::TEXT AS Available_for_rent,
lat::REAL AS latitude,
long::REAL AS longitude,
CONCAT(adresse, ', ', arrondissement, ', ', quartier, ', ', secteur_pme) AS address,
CONCAT(usage1, ', ', usage2, ', ', usage3)::TEXT AS category,
CONCAT(usage1, ', ', usage2, ', ', usage3)::TEXT AS "type",
etage AS floor_location,
suite AS unit,
CASE
	WHEN enfant = 'Non' THEN 'No'
	WHEN enfant = 'Oui' THEN 'Yes'
	ELSE NULL
END::TEXT AS Multi_Location_Occupancy,
CASE
	WHEN multiusage = 'Non' THEN 'No'
	WHEN multiusage = 'Oui' THEN 'Yes'
	ELSE NULL
END::TEXT AS multiusage,
CASE
	WHEN multioccupant = 'Non' THEN 'No'
	WHEN multioccupant = 'Oui' THEN 'Yes'
	ELSE NULL
END::TEXT AS multioccupant,
nom_centre AS center_name,
sdc_nom AS name_development_company,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-locaux-commerciaux/resource/fb2e534a-c573-45b5-b62b-8f99e3a37cd1' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL'::TEXT AS city
FROM mtl_commercial_sites


SELECT
CONCAT(LOWER(municipal_address), ', ', ward_name, ', ', LOWER(business_address), ' Toronto, ON, Canada') AS address,
operator_name AS business_name,
LOWER(category) AS category,
CASE
	WHEN operator_name IS NULL THEN 'Closed'
	ELSE 'Open'
END AS status,
NULL::DATE AS status_update_date,
(geometry::jsonb->'coordinates'->>1)::REAL AS latitude,
(geometry::jsonb->'coordinates'->>0)::REAL AS longitude,
intervention_type,
'https://open.toronto.ca/dataset/cafeto-curb-lane-parklet-cafe-locations/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'TORONTO' AS city
FROM to_food_establishments




SELECT DISTINCT
"Establishment Name" AS business_name,
CONCAT(LOWER("Establishment Address"), ', Toronto, ON, Canada') AS address,
"Establishment Type" AS category,
"Inspection Date"::DATE AS inspection_date,
"Establishment Status" AS inspection_outcome,
"Action" AS measure_taken,
"Min. Inspections Per Year" AS min_inspec_per_year,
"Infraction Details" AS infraction_details,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
'https://open.toronto.ca/dataset/dinesafe/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO'::TEXT AS city
FROM to_fines_food



SELECT
CONCAT(LOWER("Licence Address Line 1"), ', ', LOWER("Licence Address Line 3"), ', ',LOWER("Licence Address Line 2"), ' Canada') AS address,
"Client Name" AS business_name,
LOWER(category) AS category,
endorsements AS permit_activitiy,
issued::DATE AS issued_date,
"Cancel Date" AS cancel_date,
"Last Record Update"::DATE AS last_update_date,
'https://open.toronto.ca/dataset/municipal-licensing-and-standards-business-licences-and-permits/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'TORONTO' AS city
FROM to_business_permits


SELECT
"Contract Date"::DATE AS ccontract_date,
"Contract Amount"::TEXT AS contract_amount,
reason,
division,
"Supplier Name" AS supplier,
"Supplier Address" AS supplier_address,
'https://open.toronto.ca/dataset/tobids-non-competitive-contracts/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY'  AS data_refresh,
'TORONTO' AS city
FROM to_non_competitive_contracts

SELECT
TRIM(CONCAT(p.civique_debut, ', ', TRIM(p.nom_rue), ', ', a."Nom officiel", ', ', TRIM(p.suite_debut), ' Montreal, QC, Canada'))::TEXT AS address,
p.categorie_uef AS category,
p.nombre_logement::NUMERIC::INTEGER AS apt_count,
p.libelle_utilisation AS usage,
p.superficie_terrain::REAL AS lot_area_sqm,
p.superficie_batiment::REAL AS building_area_sqm,
p.etage_hors_sol::NUMERIC AS floors_above_ground,
p.annee_construction::TEXT AS construction_year,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-unites-evaluation-fonciere/resource/2b9dfc3d-91d3-48de-b32c-a2a6d9417079' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_residential_properties p
LEFT JOIN mtl_ardm a ON p.no_arrond_ile_cum = a."Code REM"



SELECT
CONCAT(TRIM(emplacement),', ',arrondissement) AS address,
date_debut::DATE AS request_permit_date,
date_emission::TEXT AS issued_date,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
description_type_demande AS category,
nb_logements::INTEGER AS apt_count,
CONCAT(description_categorie_batiment, ', nature des travaux: ', nature_travaux) AS commentaries,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-permis-construction/resource/5232a72d-235a-48eb-ae20-bb9d501300ad' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'WEEKLY'  AS data_refresh,
'MONTREAL' AS city
FROM mtl_retailer_construction_permit



SELECT
CONCAT(street_num,', ', street_type,', ',LOWER(street_name),', ',ward_grid,', ',postal,', Toronto, ON, Canada') AS address,
application_date::DATE AS request_permit_date,
issued_date::TEXT AS issued_date,
NULL::REAL AS latitude,
NULL::REAL AS longitude,
permit_type AS category,
REPLACE(dwelling_units_created,',','')::REAL AS dwelling_units_created,
REPLACE(dwelling_units_lost,',','')::REAL AS dwelling_units_lost,
CONCAT(structure_type,', ', "WORK",', ',description) AS commentaries,
status,
REPLACE(CASE WHEN est_const_cost ='DO NOT UPDATE OR DELETE THIS INFO FIELD' THEN NULL ELSE est_const_cost END,',','')::REAL AS cost_cad,
'https://open.toronto.ca/dataset/building-permits-active-permits/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM to_building_active_permits

SELECT
CONCAT(street_number,', ', street_type,', ',street_name,', ',postal,', Toronto, ON, Canada') AS address,
application_date::DATE AS application_date,
issued_date::TEXT AS issued_date,
"TYPE" AS category,
CONCAT('Use from: ',current_use,' :to: ',proposed_use) AS use_transition,
status,
'https://open.toronto.ca/dataset/building-permits-signs/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'WEEKLY'  AS data_refresh,
'TORONTO' AS city
FROM to_building_permits_signs

SELECT
"Issue Date"::TEXT AS issued_date,
"Permit Type" AS category,
"Expected End Date"::DATE AS end_date_expected,
"Actual End Date"::DATE AS end_date_effective,
CONCAT("Operating Name",'& ',"Client Name") AS contractor,
'https://open.toronto.ca/dataset/noise-exemption-permits/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY'  AS data_refresh,
'TORONTO' AS city
FROM to_noisy_places

SELECT
CONCAT(address_full,', ',postal_code,', ',municipality,', ',ward,', ',city,', ',place_name) AS address,
building_name,
CASE
  WHEN TRIM(year_install) ~ '^\d{4}$' THEN TRIM(year_install) 
  WHEN TRIM(year_install) ~ '^[A-Za-z]+\s+\d{1,2},\s+\d{4}$' THEN substring(TRIM(year_install) from '\d{4}$')
  END::NUMERIC AS year_install,
(geometry::jsonb->'coordinates'->>1)::REAL AS latitude,
(geometry::jsonb->'coordinates'->>0)::REAL AS longitude,
type_install,
size_install::REAL AS size_install,
general_use AS electricity_consumption,
'https://open.toronto.ca/dataset/renewable-energy-installations/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'UNKNOWN'::VARCHAR AS data_refresh,
'TORONTO' AS city
FROM to_green_energy_installations



SELECT
CONCAT(address, ', ', "Ward Name", ', ', city, ', Canada') AS address,
(geometry::jsonb->'coordinates'->1->0->>0)::REAL AS latitude,
(geometry::jsonb->'coordinates'->0->0->>0)::REAL AS longitude,
"Building Status" AS status,
management AS usage,
"Gross Floor Area (M2)"::REAL AS building_area_sqm,
"Gross Floor Area (FT2)"::REAL AS building_area_ft,
REPLACE("Year Built",'.0','')::INTEGER AS construction_year,
"Owner" AS contractor,
'https://open.toronto.ca/dataset/real-estate-asset-inventory/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'WEEKLY'  AS data_refresh,
'TORONTO' AS city
FROM to_real_estate_inventory


SELECT
CONCAT(site_address, ', ', wardname, ', ', 'Toronto, ON, Canada') AS address,
evaluation_completed_on::DATE AS review_date,
confirmed_storeys::REAL AS  floors_count,
confirmed_units::REAL AS unit_count,
no_of_areas_evaluated,  
REPLACE(year_built,'.0','')::INTEGER AS year_built,
REPLACE(year_registered,'.0','')::INTEGER AS year_registered,
property_type,
score::REAL AS score,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
'https://open.toronto.ca/dataset/apartment-building-evaluation/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM to_fines_buidlings


SELECT
"Intersection" AS address,
tfs_alarm_time::TEXT AS alarm_time,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
COALESCE(civilian_casualties::REAL,0)+COALESCE(tfs_firefighter_casualties::REAL,0)::REAL AS casualties,
COALESCE(count_of_persons_rescued::REAL,0)+COALESCE(estimated_number_of_persons_displaced::REAL,0)::REAL AS rescued,
estimated_dollar_loss::REAL AS estimated_loss,
ROUND(EXTRACT(EPOCH FROM (last_tfs_unit_clear_time::TIMESTAMP - tfs_arrival_time::TIMESTAMP)) / 60, 2) AS duration_minutes,
'https://open.toronto.ca/dataset/fire-incidents/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'ANNUALY' AS data_refresh,
'TORONTO' AS city
FROM to_fire_fights



SELECT
CONCAT('Numéro du poste de quartier: ', pdq) AS address,
"DATE"::DATE AS incident_datetime,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
categorie AS incident_type,
CONCAT('quart du jour: ', quart) AS details,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-actes-criminels/resource/c6f482bf-bf0f-4960-8b2f-9982c211addd' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'WEEKLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_crimes

SELECT
CONCAT(area_name,', Toronto, ON, Canada')::TEXT AS address,
"year"::INTEGER AS incident_datetime,
(SELECT AVG((point->>1)::NUMERIC) FROM jsonb_array_elements(geometry::jsonb->'coordinates'->0) AS point )::REAL AS latitude,
(SELECT AVG((point->>0)::NUMERIC) FROM jsonb_array_elements(geometry::jsonb->'coordinates'->0) AS point )::REAL AS longitude,
crime AS incident_type,
CONCAT('Count: ', "count", ', Rate: ', rate) AS details,
'https://open.toronto.ca/dataset/neighbourhood-crime-rates/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'ANNUALY' AS data_refresh,
'TORONTO' AS city
FROM to_crimes


SELECT
CONCAT(nom_qr, ', ', nom_arrond, ', Montreal, QC, Canada') AS address,
date_declaration::DATE AS inspection_datetime,
latitude::REAL,
longitude::REAL,
nbr_extermin::INTEGER AS nbr_extermin,
date_fintrait::DATE AS date_end,
date_debuttrait::DATE AS date_start,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-declarations-exterminations-punaises-de-lit/resource/ba28703e-ce85-4293-8a37-88932bf4ae93' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_bugs


SELECT
CONCAT(lieu_infraction_no_civique, ', ', lieu_infraction_rue, ', ', code_arrondissement, ', Montreal, QC, Canada') AS address,
CASE
WHEN date_jugement ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_DATE(date_jugement, 'DD-MM-YYYY')
WHEN date_jugement ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SPLIT_PART(date_jugement, '/', 1)::INT > 12 THEN TO_DATE(date_jugement, 'DD/MM/YYYY')
WHEN date_jugement ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(date_jugement, 'MM/DD/YYYY')
WHEN date_jugement ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN TO_DATE(date_jugement, 'YYYY-MM-DD')
ELSE NULL
END AS date_jugement,
CASE
WHEN date_infraction ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_DATE(date_infraction, 'DD-MM-YYYY')
WHEN date_infraction ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SPLIT_PART(date_infraction, '/', 1)::INT > 12 THEN TO_DATE(date_infraction, 'DD/MM/YYYY')
WHEN date_infraction ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(date_infraction, 'MM/DD/YYYY')
WHEN date_infraction ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN TO_DATE(date_infraction, 'YYYY-MM-DD')
ELSE NULL
END AS date_infraction,
article AS infraction_law_article,
nature_infraction ,
REGEXP_REPLACE(REPLACE(REPLACE(amende_imposee, '$', ''), ',', '.'), '[^0-9.]', '', 'g')::REAL AS fine_amount,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-liste-central-condamnations-salubrite-logements/resource/f270cb02-ca30-4b3b-96eb-f0dbdbc50ea7' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'TWICE YEARLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_residential_fines

SELECT
CONCAT(LOWER(addrfull),', Tonronto, ON, Canada') AS address,
estname AS establishment_name,
srvtype AS category,
(geometry::jsonb->'coordinates'->>1)::REAL AS latitude,
(geometry::jsonb->'coordinates'->>0)::REAL AS longitude,
coalesce(observation, infcategory) AS violation_type,
CONCAT('Infraction Level: ', inftype) AS details,
insstatus AS status,
insdate::DATE AS status_date,
actiondesc AS actions,
'https://open.toronto.ca/dataset/bodysafe/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM to_barbers_inspections


SELECT
CONCAT(TRIM("Institution Address"),', ','Toronto, ON, Canada') AS address,
"Institution Name" AS establishment_name,
"Date Outbreak Began"::DATE AS inspection_datetime,
COALESCE("Causative Agent-1","Causative Agent-2") AS violation_type,
CASE WHEN active ='Y' THEN 'Active' ELSE 'Inactive' END::TEXT AS status,
"Date Outbreak Began"::DATE AS status_date,
'https://open.toronto.ca/dataset/outbreaks-in-toronto-healthcare-institutions/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM to_health_outbreaks

SELECT
CONCAT(propertyaddress,', ',propertyward,', Toronto, ON, Canada') AS address,
inspections_opendate::DATE AS inspection_started,
inspections_closeddate::DATE AS inspection_ended,
violation_description AS violation_type,
REPLACE(violations_item_number,'.0',''):: INTEGER AS violations_count,
'https://open.toronto.ca/dataset/highrise-residential-fire-inspection-results/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY' AS data_refresh,
'TORONTO' AS city
FROM to_fire_inspections



SELECT
site_name,
"owner" AS owner_name,
CONCAT(site_name,', ', district,' Toronto, ON, Canada') AS address,
(geometry::jsonb->'coordinates'->1)::REAL AS latitude,
(geometry::jsonb->'coordinates'->0)::REAL AS longitude,
CASE
	WHEN "statement" ILIKE '%sale%' THEN 'sale'
	WHEN "statement" ILIKE '%lease%' THEN 'lease'
	ELSE 'other'
END AS open_for,
"statement",
status,
CONCAT(property_type_primary,'; ',property_type_secondary) AS category,
planning_status,
    -- Extract numeric value from total_site_area_acres (handling units/text)
    CASE
        WHEN total_site_area_acres ~ '\d' THEN 
            REGEXP_REPLACE(total_site_area_acres, '[^\d.]', '', 'g') 
        ELSE NULL
    END AS total_site_area_acres_numeric,

    -- Standardize site_area_range_1 into interval format (e.g., "0 - 5" → "0-5")
    CASE
        WHEN site_area_range_1 ~ '\d' THEN REPLACE(site_area_range_1, ' ', '')
        ELSE NULL
    END AS site_area_range_1_interval,

    -- Extract numeric value from frontage_feet (first occurrence)
    CASE
        WHEN frontage_feet ~ '\d' THEN 
            REGEXP_REPLACE(SUBSTRING(frontage_feet FROM '(\d+\.?\d*)'), '[^\d.]', '', 'g') 
        ELSE NULL
    END AS frontage_feet_numeric,

    -- Convert remaining columns to intervals or text (ensuring consistent output type)
    CASE
        WHEN site_area_range2 ~ '\d' THEN REPLACE(site_area_range2, ' ', '')
        ELSE NULL
    END AS site_area_range2_interval,

    CASE
        WHEN sq_ft_available ~ '\d' THEN 
            CASE
                -- Return ranges as text (e.g., "100001-300000")
                WHEN sq_ft_available ~ '-' THEN REPLACE(sq_ft_available, ' ', '')
                -- Return numbers as text (to match the CASE type)
                ELSE REGEXP_REPLACE(sq_ft_available, '[^\d.]', '', 'g')
            END
        ELSE NULL
    END AS sq_ft_available_clean,

    -- Predefined range columns (clean formatting)
    REPLACE(sq_ft_available_range1, ' ', '') AS sq_ft_available_range1_interval,
    REPLACE(sq_ft_available_range2, ' ', '') AS sq_ft_available_range2_interval,

CONCAT(phone3,', ', email3) AS persone3,
CONCAT(company2,', ',phone2,', ', email2) AS person2,
CONCAT(company1,', ', phone1,', ',email1) AS person1,
"comments",
'https://open.toronto.ca/dataset/toronto-signature-sites/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY'  AS data_refresh,
'TORONTO' AS city
FROM to_sites_opportunities

SELECT 
CONCAT(address_full,', ',postal_code,', ',ward_name,', ', city,', Canada') AS address,
bike_capacity::INTEGER AS bike_capacity,
change_room,
vending_machine,
tools_pump,
yr_install::INTEGER AS yr_install,
(geometry::jsonb->'coordinates'->>1)::REAL AS latitude,
(geometry::jsonb->'coordinates'->>0)::REAL AS longitude,
'https://open.toronto.ca/dataset/bicycle-parking-bike-stations-indoor/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'DAILY'  AS data_refresh,
'TORONTO' AS city
FROM to_cycle_parking

SELECT 
CONCAT(nom_secteur_sse,', ', arr_secteur) AS region,
date_action_shp::TIMESTAMP AS date_infraction,
total_recu::REAL AS amount,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-vignettes-stationnement/resource/e7df09fb-af5a-476f-861e-ea23777749b5' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'WEEKLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_parking_fines


SELECT
CONCAT(no_civ_lie,', ',prefix_tem,', ', nom_temp, ', ', desc_lieu) AS Police_office,
latitude::REAL AS latitude,
longitude::REAL AS longitude,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-carte-postes-quartier/resource/c9f296dd-596e-48ed-9c76-37230b2c916d' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'YEARLY' AS data_refresh,
'MONTREAL' AS city
FROM mtl_police_offices


WITH original_data AS (
  SELECT 
    *,
    ROW_NUMBER() OVER () AS row_id  -- Unique identifier for original rows
  FROM to_apt_registrations
)
SELECT
  row_id,
  CONCAT(TRIM(site_address),', Toronto, ON, Canada') AS address,
property_type AS category,
confirmed_storeys::INTEGER AS floors_count,
confirmed_units::INTEGER AS unit_counts,
prop_management_company_name AS management_company,
REPLACE(year_registered,'.0',''):: INTEGER AS year_registered,
REPLACE(year_of_replacement,'.0',''):: INTEGER AS year_of_replacement,
REPLACE(year_built,'.0',''):: INTEGER AS year_built,
date_of_last_inspection_by_tssa::DATE AS last_inspection_date,
  building_part,
  attribute,
  'https://open.toronto.ca/dataset/apartment-building-registration/' AS data_source,
'2025-04-29'::DATE AS data_pull_date,
'MONTHLY'  AS data_refresh,
'TORONTO' AS city
FROM original_data
CROSS JOIN LATERAL (
  VALUES
    ('air_conditioning_type', air_conditioning_type::TEXT),
    ('amenities_available', amenities_available::TEXT),
    ('annual_fire_alarm_test_records', annual_fire_alarm_test_records::TEXT),
    ('annual_fire_pump_flow_test_records', annual_fire_pump_flow_test_records::TEXT),
    ('approved_fire_safety_plan', approved_fire_safety_plan::TEXT),
    ('balconies', balconies::TEXT),
    ('barrier_free_accessibilty_entr', barrier_free_accessibilty_entr::TEXT),
    ('bike_parking', bike_parking::TEXT),
	('description_of_child_play_area', description_of_child_play_area::TEXT),
	('description_of_indoor_exercise_room', description_of_indoor_exercise_room::TEXT),
	('description_of_outdoor_rec_facilities', description_of_outdoor_rec_facilities::TEXT),
	('elevator_parts_replaced', elevator_parts_replaced::TEXT),
    ('elevator_status', elevator_status::TEXT),
    ('emerg_power_supply_test_records', emerg_power_supply_test_records::TEXT),
    ('exterior_fire_escape', exterior_fire_escape::TEXT),
    ('facilities_available', facilities_available::TEXT),
    ('fire_alarm', fire_alarm::TEXT),
    ('garbage_chutes', garbage_chutes::TEXT),
    ('green_bin_location', green_bin_location::TEXT),
    ('heating_equipment_status', heating_equipment_status::TEXT),
	('heating_equipment_year_installed', heating_equipment_year_installed::TEXT),
    ('heating_type', heating_type::TEXT),
    ('Indoor_garbage_storage_area', Indoor_garbage_storage_area::TEXT),
    ('Intercom', Intercom::TEXT),
    ('is_there_a_cooling_room', is_there_a_cooling_room::TEXT),
    ('Is_there_emergency_power', Is_there_emergency_power::TEXT),
    ('laundry_room', laundry_room::TEXT),
    ('laundry_room_hours_of_operation', laundry_room_hours_of_operation::TEXT),
    ('laundry_room_location', laundry_room_location::TEXT),
	('locker_or_storage_room', locker_or_storage_room::TEXT),
	('no_barrier_free_accessble_units', no_barrier_free_accessble_units::TEXT),
	('no_of_accessible_parking_spaces', no_of_accessible_parking_spaces::TEXT),
	('no_of_elevators', no_of_elevators::TEXT),
	('no_of_laundry_room_machines', no_of_laundry_room_machines::TEXT),
	('non_smoking_building', non_smoking_building::TEXT),
	('outdoor_garbage_storage_area', outdoor_garbage_storage_area::TEXT),
	('parking_type', parking_type::TEXT),
	('pet_restrictions', pet_restrictions::TEXT),
	('pets_allowed', pets_allowed::TEXT),
	('property_type', property_type::TEXT),
	('recycling_bins_location', recycling_bins_location::TEXT),
	('separate_gas_meters_each_unit', separate_gas_meters_each_unit::TEXT),
	('separate_hydro_meter_each_unit', separate_hydro_meter_each_unit::TEXT),
	('separate_water_meters_ea_unit', separate_water_meters_ea_unit::TEXT),
	('sprinkler_system', sprinkler_system::TEXT)
) AS unpivoted(building_part, attribute);


