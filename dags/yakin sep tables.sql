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
   '{{ data_pull_date }}'::DATE AS data_pull_date,
   'MONTHLY' AS data_refresh,
   'MONTREAL'::TEXT AS city
   FROM mtl_fines_food