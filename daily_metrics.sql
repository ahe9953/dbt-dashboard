/*
  Dosya Adı: daily_metrics.sql
  Açıklama: Bu model, ham olay verilerini günlük, ülke ve platform bazında kümülatif metriklere dönüştürür.
*/

/* Bu komut dbt'nin "materialized='table'" ayarının yaptığı işi yapar.
   Sorgu sonucunu 'daily_metrics' adında bir tablo olarak kaydeder.
*/

CREATE OR REPLACE TABLE `project-8cb6cff5-bf7b-4412-b2a.vert1.daily_metrics` AS

WITH daily_data AS (
    SELECT
        event_date,
        -- Boş string veya sadece boşluk olanları NULL'a çevirip sonra COALESCE uyguluyoruz
        COALESCE(NULLIF(TRIM(country), ''), 'Unknown') AS country,
        platform,
        -- Metrikler için gerekli temel hesaplamalar
        COUNT(DISTINCT user_id) AS dau,
        SUM(iap_revenue) AS total_iap_revenue,
        SUM(ad_revenue) AS total_ad_revenue,
        SUM(match_start_count) AS matches_started,
        SUM(match_end_count) AS matches_ended,
        SUM(victory_count) AS total_victories,
        SUM(defeat_count) AS total_defeats,
        SUM(server_connection_error) AS total_server_errors
    FROM
        `project-8cb6cff5-bf7b-4412-b2a.vert1.raw_user_metrics` -- dbt source tanımınıza göre burayı güncelleyin
    GROUP BY
        1, 2, 3
)

SELECT
    event_date,
    country,
    platform,
    dau,
    total_iap_revenue,
    total_ad_revenue,
    -- ARPDAU: (IAP + Ad Revenue) / DAU
    SAFE_DIVIDE((total_iap_revenue + total_ad_revenue), dau) AS arpdau,
    
    matches_started,
    
    -- Match per DAU
    SAFE_DIVIDE(matches_started, dau) AS match_per_dau,
    
    -- Win Ratio
    SAFE_DIVIDE(total_victories, matches_ended) AS win_ratio,
    
    -- Defeat Ratio
    SAFE_DIVIDE(total_defeats, matches_ended) AS defeat_ratio,
    
    -- Server Error per DAU
    SAFE_DIVIDE(total_server_errors, dau) AS server_error_per_dau

FROM daily_data
