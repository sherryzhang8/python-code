{% set target_database_name = 'adhoc' if test_mode else 'dw' %}
{% set s3_bucket_name = 's3://dw' %}
{% set target_table_name = 'sd_requests_agg' if test_mode else 'sd_requests_agg' %}

--DROP          TABLE IF     EXISTS {{target_database_name}}.{{target_table_name}};
CREATE EXTERNAL TABLE IF NOT EXISTS {{target_database_name}}.{{target_table_name}}
(
  `r` decimal(38,19),
  `r_nines` double,
  `total_count` bigint)
  PARTITIONED BY( ds STRING )
  STORED AS PARQUET
LOCATION
  '{{s3_bucket_name}}/{{target_database_name}}/{{target_table_name}}'
;

-- HAProxy serves as our primary ingress tier at the moment
WITH hap_log_counts AS (

    SELECT
        ds,
        -- Only select requests with valid HTTP codes (ignoring -1)
        COUNT(CASE WHEN http_status_code BETWEEN 100 AND 499 THEN http_status_code END) AS success_count,
        COUNT(CASE WHEN http_status_code BETWEEN 100 AND 599 THEN http_status_code END) AS total_count
    FROM
        dw.haproxy_logs
    WHERE
        ds = '{{ds}}'
        -- We look at edge ingress only
        AND server like 'hap-%'

    GROUP BY ds
),
-- Legacy ingress tier, soon to be deprecated.
cloudfront_log_counts AS (
    SELECT
        ds,
        COUNT(CASE WHEN NOT sc_status BETWEEN 500 AND 599 THEN sc_status END) AS success_count,
        COUNT(1) AS total_count
    FROM
        dw.aws_cloudfront_logs
    WHERE
        ds = '{{ds}}'

    GROUP BY ds
)

INSERT OVERWRITE TABLE {{target_database_name}}.{{target_table_name}}
PARTITION (ds = '{{ds}}')
SELECT
    (COALESCE(ha.success_count,0) + COALESCE(cf.success_count,0)) / (1.0 * COALESCE(ha.total_count,0) + COALESCE(cf.total_count,0)) AS R,
    -log10(1.0 - ((COALESCE(ha.success_count,0) + COALESCE(cf.success_count,0)) / (1.0 * COALESCE(ha.total_count,0) + COALESCE(cf.total_count,0)))) AS R_nines,
    COALESCE(ha.total_count,0) + COALESCE(cf.total_count,0) AS total_count
FROM
    hap_log_counts ha
FULL OUTER JOIN
    cloudfront_log_counts cf
ON
    ha.ds=cf.ds
;
