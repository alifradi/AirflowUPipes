DROP TABLE IF EXISTS public_transformed_data.tf_mtl_fines_food CASCADE;
CREATE TABLE public_transformed_data.tf_mtl_fines_food AS
SELECT DISTINCT
   "etablissement" AS business_name,
   TRIM("adresse") AS address,
   "categorie" AS category,
   "proprietaire" AS owner_name,
   "statut" AS status,
   "date"::DATE AS infraction_date,
   "date_statut"::DATE AS status_update_date,
   "date_jugement"::DATE AS date_jugement,
   "description",
   'https://www.donneesquebec.ca/recherche/dataset/vmtl-inspection-aliments-contrevenants/resource/7f939a08-be8a-45e1-b208-d8744dca8fc6' AS data_source,
   CURRENT_DATE AS data_pull_date,
   'monthly' AS data_refresh,
   'montreal' AS city
FROM quebec_data.mtl_fines_food;
 -- Montreal Fire Fights
SELECT
CONCAT("NOM_ARROND", ', ', "NOM_VILLE", ', Montreal, QC, Canada') AS address,
"CREATION_DATE_TIME"::TIMESTAMP AS incident_datetime,
"INCIDENT_TYPE_DESC" AS description,
"DESCRIPTION_GROUPE" AS category,
CASE WHEN "CASERNE" ~ '^[0-9.]+$' THEN "CASERNE"::NUMERIC ELSE NULL END AS caserne,
CASE WHEN "DIVISION" ~ '^[0-9.]+$' THEN "DIVISION"::NUMERIC ELSE NULL END AS division,
CASE WHEN "NOMBRE_UNITES" ~ '^[0-9.]+$' THEN "NOMBRE_UNITES"::NUMERIC ELSE NULL END AS unit_count,
CASE WHEN "LATITUDE" ~ '^[0-9.]+$' THEN "LATITUDE"::REAL ELSE NULL END AS latitude,
CASE WHEN "LONGITUDE" ~ '^[0-9.]+$' THEN "LONGITUDE"::REAL ELSE NULL END AS longitude,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-interventions-service-securite-incendie-montreal/resource/71e86320-e35c-4b4c-878a-e52124294355' AS data_source,
CURRENT_DATE AS data_pull_date,
'WEEKLY' AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_fire_fights;

-- Montreal Passenger Counts
SELECT
CASE WHEN COALESCE("Date", '') <> '' AND COALESCE("Periode", '') <> '' AND "Date" <> 'None' AND "Periode" <> 'None' THEN CONCAT("Date", ' ', "Periode")::TIMESTAMP ELSE NULL END AS record_datetime,
"Description_Code_Banque" AS location_description,
("NBLT"::NUMERIC + "NBT"::NUMERIC + "NBRT"::NUMERIC +
"SBLT"::NUMERIC + "SBT"::NUMERIC + "SBRT"::NUMERIC +
"EBLT"::NUMERIC + "EBT"::NUMERIC + "EBRT"::NUMERIC +
"WBLT"::NUMERIC + "WBT"::NUMERIC + "WBRT"::NUMERIC +
"Approche_Nord"::NUMERIC + "Approche_Sud"::NUMERIC +
"Approche_Est"::NUMERIC + "Approche_Ouest"::NUMERIC)::NUMERIC AS total_passengers,
"Longitude"::REAL,
"Latitude"::REAL,    
'https://www.donneesquebec.ca/recherche/dataset/vmtl-comptage-vehicules-pietons/resource/f82f00c0-baed-4fa1-8b01-6ed60146d102' AS data_source,
 CURRENT_DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL'::TEXT AS city
FROM quebec_data.mtl_Passenger_count;

-- Montreal Food Fines
SELECT DISTINCT
"etablissement" AS business_name,
TRIM("adresse") AS address,
"categorie" AS category,
"proprietaire" AS owner_name,
"statut" AS status,
"date"::DATE AS infraction_date,
"date_statut"::DATE AS status_update_date,
"date_jugement"::DATE AS date_jugement,
"description",
'https://www.donneesquebec.ca/recherche/dataset/vmtl-inspection-aliments-contrevenants/resource/7f939a08-be8a-45e1-b208-d8744dca8fc6' AS data_source,
 CURRENT_DATE AS data_pull_date,
'monthly' AS data_refresh,
'montreal' AS city
FROM quebec_data.mtl_fines_food;

-- Montreal Food Establishments
SELECT
CONCAT(TRIM("address"),', ',"city",', ',"state") AS address,
"name" AS business_name,
"type" AS category,
"statut" AS status,
"date_statut"::DATE AS status_update_date,
NULLIF("latitude", 'None')::REAL AS latitude,
NULLIF("longitude", 'None')::REAL AS longitude,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-etablissements-alimentaires/resource/28a4957d-732e-48f9-8adb-0624867d9bb0' AS data_source,
CURRENT_DATE AS data_pull_date,
'monthly' AS data_refresh,
'montreal' AS city
FROM quebec_data.mtl_food_establishments;

-- Montreal Commercial Sites
SELECT
"DATE_CREATION"::DATE AS creation_date,
"NOM_ETAB" AS business_name,
CASE
	WHEN "VACANT_A_LOUER" = 'Non' THEN 'No'
	ELSE 'Yes'
END::TEXT AS available_for_rent,
"LAT"::REAL AS latitude,
"LONG"::REAL AS longitude,
CONCAT("ADRESSE", ', ', "ARRONDISSEMENT", ', ', "QUARTIER", ', ', "SECTEUR_PME") AS address,
CONCAT("USAGE1", ', ', "USAGE2", ', ', "USAGE3")::TEXT AS category,
CONCAT("USAGE1", ', ', "USAGE2", ', ', "USAGE3")::TEXT AS type,
"ETAGE" AS floor_location,
"SUITE" AS unit,
CASE
	WHEN "ENFANT" = 'Non' THEN 'No'
	WHEN "ENFANT" = 'Oui' THEN 'Yes'
	ELSE NULL
END::TEXT AS multi_location_occupancy,
CASE
	WHEN "MULTIUSAGE" = 'Non' THEN 'No'
	WHEN "MULTIUSAGE" = 'Oui' THEN 'Yes'
	ELSE NULL
END::TEXT AS multiusage,
CASE
	WHEN "MULTIOCCUPANT" = 'Non' THEN 'No'
	WHEN "MULTIOCCUPANT" = 'Oui' THEN 'Yes'
	ELSE NULL
END::TEXT AS multioccupant,
"NOM_CENTRE" AS center_name,
"SDC_NOM" AS name_development_company,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-locaux-commerciaux/resource/fb2e534a-c573-45b5-b62b-8f99e3a37cd1' AS data_source,
CURRENT_DATE AS data_pull_date,
'monthly' AS data_refresh,
'montreal' AS city
FROM quebec_data.mtl_commercial_sites;

-- Residential Properties
SELECT
TRIM(CONCAT(p."CIVIQUE_DEBUT", ', ', TRIM(p."NOM_RUE"), ', ', a."Nom officiel", ', ', TRIM(p."SUITE_DEBUT"), ' Montreal, QC, Canada'))::TEXT AS address,
p."CATEGORIE_UEF" AS category,
CASE WHEN p."NOMBRE_LOGEMENT" ~ '^[0-9]+$' THEN p."NOMBRE_LOGEMENT"::INTEGER ELSE NULL END AS apt_count,
p."LIBELLE_UTILISATION" AS usage,
CASE WHEN p."SUPERFICIE_TERRAIN" ~ '^[0-9.]+$' THEN p."SUPERFICIE_TERRAIN"::REAL ELSE NULL END AS lot_area_sqm,
CASE WHEN p."SUPERFICIE_BATIMENT" ~ '^[0-9.]+$' THEN p."SUPERFICIE_BATIMENT"::REAL ELSE NULL END AS building_area_sqm,
CASE WHEN p."ETAGE_HORS_SOL" ~ '^[0-9.]+$' THEN p."ETAGE_HORS_SOL"::NUMERIC ELSE NULL END AS floors_above_ground,
p."ANNEE_CONSTRUCTION"::TEXT AS construction_year,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-unites-evaluation-fonciere/resource/2b9dfc3d-91d3-48de-b32c-a2a6d9417079' AS data_source,
CURRENT_DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_Residential_properties p
LEFT JOIN quebec_data.mtl_ardm a ON p."NO_ARROND_ILE_CUM" = a."Code REM";

-- Construction Permits
SELECT
CONCAT(TRIM("emplacement"),', ',"arrondissement") AS address,
"date_debut"::DATE AS request_permit_date,
"date_emission"::TEXT AS issued_date,
NULLIF("latitude", 'None')::REAL AS latitude,
NULLIF("longitude", 'None')::REAL AS longitude,
"description_type_demande" AS category,
NULLIF("nb_logements", 'None')::INTEGER AS apt_count,
CONCAT("description_categorie_batiment", ', nature des travaux: ', "nature_travaux") AS commentaries,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-permis-construction/resource/5232a72d-235a-48eb-ae20-bb9d501300ad' AS data_source,
CURRENT_DATE AS data_pull_date,
'WEEKLY'  AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_retailer_construction_permit;

-- Montreal Crimes
SELECT
CONCAT('Numéro du poste de quartier: ', "PDQ") AS address,
NULLIF("DATE", 'None')::DATE AS incident_datetime,
NULLIF("LATITUDE", 'None')::REAL AS latitude,
NULLIF("LONGITUDE", 'None')::REAL AS longitude,
"CATEGORIE" AS incident_type,
CONCAT('quart du jour: ', "QUART") AS details,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-actes-criminels/resource/c6f482bf-bf0f-4960-8b2f-9982c211addd' AS data_source,
CURRENT_DATE AS data_pull_date,
'weekly' AS data_refresh,
'montreal' AS city
FROM quebec_data.mtl_crimes;

-- Bed Bug Reports
SELECT
CONCAT("NOM_QR", ', ', "NOM_ARROND", ', Montreal, QC, Canada') AS address,
NULLIF("DATE_DECLARATION", 'None')::DATE AS inspection_datetime,
NULLIF("LATITUDE", 'None')::REAL AS latitude,
NULLIF("LONGITUDE", 'None')::REAL AS longitude,
NULLIF("NBR_EXTERMIN", 'None')::INTEGER AS nbr_extermin,
NULLIF("DATE_FINTRAIT", 'None')::DATE AS date_end,
NULLIF("DATE_DEBUTTRAIT", 'None')::DATE AS date_start,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-declarations-exterminations-punaises-de-lit/resource/ba28703e-ce85-4293-8a37-88932bf4ae93' AS data_source,
CURRENT_DATE AS data_pull_date,
'MONTHLY' AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_bugs;

-- Residential Fines
SELECT
CONCAT("lieu_infraction_no_civique", ', ', "lieu_infraction_rue", ', ', "code_arrondissement", ', Montreal, QC, Canada') AS address,
CASE
WHEN "date_jugement" ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_DATE("date_jugement", 'DD-MM-YYYY')
WHEN "date_jugement" ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SPLIT_PART("date_jugement", '/', 1)::INT > 12 THEN TO_DATE("date_jugement", 'DD/MM/YYYY')
WHEN "date_jugement" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE("date_jugement", 'MM/DD/YYYY')
WHEN "date_jugement" ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN TO_DATE("date_jugement", 'YYYY-MM-DD')
ELSE NULL
END AS date_jugement,
CASE
WHEN "date_infraction" ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_DATE("date_infraction", 'DD-MM-YYYY')
WHEN "date_infraction" ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND SPLIT_PART("date_infraction", '/', 1)::INT > 12 THEN TO_DATE("date_infraction", 'DD/MM/YYYY')
WHEN "date_infraction" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE("date_infraction", 'MM/DD/YYYY')
WHEN "date_infraction" ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN TO_DATE("date_infraction", 'YYYY-MM-DD')
ELSE NULL
END AS date_infraction,
"article" AS infraction_law_article,
"nature_infraction",
CASE 
    WHEN TRIM("amende_imposee") = '' THEN NULL 
    ELSE NULLIF(REGEXP_REPLACE(REPLACE(REPLACE("amende_imposee", '$', ''), ',', '.'), '[^0-9.]', '', 'g'), '')::REAL 
END AS fine_amount,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-liste-central-condamnations-salubrite-logements/resource/f270cb02-ca30-4b3b-96eb-f0dbdbc50ea7' AS data_source,
CURRENT_DATE AS data_pull_date,
'TWICE YEARLY' AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_residential_fines;

-- Parking Fines
SELECT
CONCAT("NOM_SECTEUR_SSE",', ', "ARR_SECTEUR",', Montreal, QC, Canada') AS address,
NULLIF("DATE_ACTION_SHP", 'None')::TIMESTAMP AS date_infraction,
NULLIF("TOTAL_RECU", 'None')::REAL AS amount,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-vignettes-stationnement/resource/e7df09fb-af5a-476f-861e-ea23777749b5' AS data_source,
CURRENT_DATE AS data_pull_date,
'WEEKLY' AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_parking_fines;

-- Police Offices
SELECT
CONCAT("NO_CIV_LIE",', ',"PREFIX_TEM",', ', "NOM_TEMP", ', ', "DESC_LIEU") AS police_office,
NULLIF("LATITUDE", 'None')::REAL AS latitude,
NULLIF("LONGITUDE", 'None')::REAL AS longitude,
'https://www.donneesquebec.ca/recherche/dataset/vmtl-carte-postes-quartier/resource/c9f296dd-596e-48ed-9c76-37230b2c916d' AS data_source,
CURRENT_DATE AS data_pull_date,
'YEARLY' AS data_refresh,
'MONTREAL' AS city
FROM quebec_data.mtl_police_offices;
