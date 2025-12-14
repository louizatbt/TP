-- ============================================================
-- TP VéloCity – Cycle de Vie de la Donnée : de la Source au Dashboard
-- Script SQL UNIQUE (Partie 2 + Partie 4)
-- Etudiant(e) : Louiza Tabet
-- Schéma : analytics_Louiza_Anis
-- Base : postgres
-- ============================================================

-- -----------------------------
-- (Optionnel) Nettoyage pour rejouer le script
-- -----------------------------
CREATE SCHEMA IF NOT EXISTS analytics_Louiza_Anis;

DROP TABLE IF EXISTS analytics_Louiza_Anis.gold_daily_activity CASCADE;
DROP TABLE IF EXISTS analytics_Louiza_Anis.silver_bike_rentals CASCADE;
DROP TABLE IF EXISTS analytics_Louiza_Anis.silver_user_accounts CASCADE;
DROP TABLE IF EXISTS analytics_Louiza_Anis.silver_bike_stations CASCADE;
DROP TABLE IF EXISTS analytics_Louiza_Anis.silver_bikes CASCADE;
DROP TABLE IF EXISTS analytics_Louiza_Anis.silver_subscriptions CASCADE;
DROP TABLE IF EXISTS analytics_Louiza_Anis.silver_cities CASCADE;

-- ============================================================
-- PARTIE 2 — Couche SILVER (nettoyage + typage)
-- ============================================================

-- 1) Villes
CREATE TABLE analytics_Louiza_Anis.silver_cities AS
SELECT
    city_id,
    TRIM(city_name) AS city_name,
    TRIM(region)    AS region
FROM raw.cities;

-- 2) Abonnements
CREATE TABLE analytics_Louiza_Anis.silver_subscriptions AS
SELECT
    subscription_id,
    TRIM(subscription_type) AS subscription_type,
    price_eur::numeric      AS price_eur
FROM raw.subscriptions;

-- 3) Vélos
CREATE TABLE analytics_Louiza_Anis.silver_bikes AS
SELECT
    bike_id,
    LOWER(TRIM(bike_type))          AS bike_type,
    TRIM(model_name)                AS model_name,
    commissioning_date::date        AS commissioning_date,
    LOWER(TRIM(status))             AS status
FROM raw.bikes;

-- 4) Stations (cast sécurisé latitude/longitude à cause de valeurs "coord_lat" etc.)
CREATE TABLE analytics_Louiza_Anis.silver_bike_stations AS
SELECT
    station_id,
    TRIM(station_name) AS station_name,
    CASE
        WHEN latitude ~ '^-?[0-9]+(\.[0-9]+)?$' THEN latitude::numeric
        ELSE NULL
    END AS latitude,
    CASE
        WHEN longitude ~ '^-?[0-9]+(\.[0-9]+)?$' THEN longitude::numeric
        ELSE NULL
    END AS longitude,
    capacity::integer AS capacity,
    city_id
FROM raw.bike_stations;

-- 5) Utilisateurs (conversion explicite JJ/MM/AAAA -> DATE)
CREATE TABLE analytics_Louiza_Anis.silver_user_accounts AS
SELECT
    user_id,
    TRIM(first_name)      AS first_name,
    TRIM(last_name)       AS last_name,
    LOWER(TRIM(email))    AS email,
    CASE
        WHEN birthdate ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        THEN TO_DATE(birthdate, 'DD/MM/YYYY')
        ELSE NULL
    END AS birthdate,
    CASE
        WHEN registration_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        THEN TO_DATE(registration_date, 'DD/MM/YYYY')
        ELSE NULL
    END AS registration_date,
    subscription_id
FROM raw.user_accounts;

-- 6) Locations (durée en minutes + filtre trajets < 2 minutes)
CREATE TABLE analytics_Louiza_Anis.silver_bike_rentals AS
SELECT
    rental_id,
    bike_id,
    user_id,
    start_station_id,
    end_station_id,
    start_t::timestamp AS start_time,
    end_t::timestamp   AS end_time,
    EXTRACT(EPOCH FROM (end_t::timestamp - start_t::timestamp)) / 60.0 AS duration_minutes
FROM raw.bike_rentals
WHERE start_t IS NOT NULL
  AND end_t IS NOT NULL
  AND EXTRACT(EPOCH FROM (end_t::timestamp - start_t::timestamp)) / 60.0 >= 2;

-- ============================================================
-- PARTIE 2 — Couche GOLD (table agrégée pour Metabase)
-- ============================================================

CREATE TABLE analytics_Louiza_Anis.gold_daily_activity AS
SELECT
    DATE(r.start_time)                AS activity_date,
    c.city_name                       AS city_name,
    s.station_name                    AS station_name,
    b.bike_type                       AS bike_type,
    sub.subscription_type             AS subscription_type,
    COUNT(*)                          AS total_rentals,
    AVG(r.duration_minutes)           AS average_duration_minutes,
    COUNT(DISTINCT u.user_id)         AS unique_users
FROM analytics_Louiza_Anis.silver_bike_rentals r
JOIN analytics_Louiza_Anis.silver_user_accounts u
  ON r.user_id = u.user_id
JOIN analytics_Louiza_Anis.silver_subscriptions sub
  ON u.subscription_id = sub.subscription_id
JOIN analytics_Louiza_Anis.silver_bikes b
  ON r.bike_id = b.bike_id
JOIN analytics_Louiza_Anis.silver_bike_stations s
  ON r.start_station_id = s.station_id
JOIN analytics_Louiza_Anis.silver_cities c
  ON s.city_id = c.city_id
GROUP BY
    DATE(r.start_time),
    c.city_name,
    s.station_name,
    b.bike_type,
    sub.subscription_type;

-- ============================================================
-- PARTIE 4 — Sécurité & Gouvernance
-- ============================================================

-- 4.1 — Rôle marketing_user : accès uniquement à la table GOLD
-- (On suppose que le rôle marketing_user est déjà créé par l'admin.)
GRANT USAGE ON SCHEMA analytics_Louiza_Anis TO marketing_user;
GRANT SELECT ON TABLE analytics_Louiza_Anis.gold_daily_activity TO marketing_user;

-- Interdiction d'accès au RAW
REVOKE ALL ON SCHEMA raw FROM marketing_user;
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM marketing_user;

-- 4.2 — Row Level Security : manager_lyon ne voit que les lignes de Lyon
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'manager_lyon') THEN
        CREATE ROLE manager_lyon LOGIN PASSWORD 'changeme';
    END IF;
END$$;

GRANT USAGE ON SCHEMA analytics_Louiza_Anis TO manager_lyon;
GRANT SELECT ON TABLE analytics_Louiza_Anis.gold_daily_activity TO manager_lyon;

ALTER TABLE analytics_Louiza_Anis.gold_daily_activity ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_policies
        WHERE schemaname = 'analytics_Louiza_Anis'
          AND tablename  = 'gold_daily_activity'
          AND policyname = 'lyon_policy'
    ) THEN
        DROP POLICY lyon_policy ON analytics_Louiza_Anis.gold_daily_activity;
    END IF;
END$$;

CREATE POLICY lyon_policy
ON analytics_Louiza_Anis.gold_daily_activity
FOR SELECT
TO manager_lyon
USING (city_name = 'Lyon');

-- ============================================================
-- FIN DU SCRIPT
-- ============================================================
