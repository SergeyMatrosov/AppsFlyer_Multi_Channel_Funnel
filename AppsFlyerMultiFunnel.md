WITH
  clicks AS (
    SELECT
        '2022-03-29 15:03:02 UTC' AS event_time,
        'click' AS event_name,
        'cpanetwork1' AS media_source,
        'aaa111-bbb111' AS advertising_id
    UNION ALL 
     SELECT '2022-03-30 16:15:24 UTC', 'click', 'cpanetwork2', 'aaa111-bbb111'
    UNION ALL
     SELECT '2022-04-01 17:15:00 UTC', 'click', 'cpanetwork3', 'aaa111-bbb111'
    UNION ALL
     SELECT '2022-03-31 16:03:47 UTC', 'click', 'mobtop', 'aaa111-bbb111' 
    UNION ALL
     SELECT '2022-03-28 17:15:23 UTC', 'click', 'cpanetwork1', 'ccc555-ddd555'
    UNION ALL
     SELECT '2022-03-30 18:15:32 UTC', 'click', 'cpanetwork2', 'ccc555-ddd555'
  ),
  installs AS (
    SELECT 
      '2022-03-31 18:03:47 UTC' AS install_time,
      'install' AS event_name, 
      'googleads' AS media_source,
      '2022-03-31 17:03:47 UTC' AS contributor_1_touch_time,
      'twitter' AS contributor_1_media_source,
      '2022-03-31 16:03:47 UTC' AS contributor_2_touch_time,
      'mobtop' AS contributor_2_media_source,
      '2022-03-31 15:03:47 UTC' AS contributor_3_touch_time,
      'organic' AS contributor_3_media_source,
      '111-222' AS appsflyer_id,
      'aaa111-bbb111' AS advertising_id
  UNION ALL
    SELECT '2022-03-30 18:05:47 UTC', 'install', 'organic', null, null, null, null, null, null, '333-444', 'bbb222-ccc222' 
  UNION ALL
    SELECT '2022-03-29 17:06:50 UTC', 'install', 'facebook', '2022-03-28 15:06:50 UTC', 'mobtop', null, null, null, null, '555-666', 'ccc555-ddd555'
  UNION ALL
  --must be deleted due to fraudulent install
    SELECT '2022-03-29 17:07:50 UTC', 'install', 'fraudcpa', null, null, null, null, null, null, '777-888', 'eee777-fff777'
  UNION ALL
  --media_source will be changed due to fraudulent install on organic
    SELECT '2022-03-29 17:08:50 UTC', 'install', 'fraudcpa', null, null, null, null, null, null, '999-100', 'ggg999-iii999'
  UNION ALL
  --media_source will be changed due to fraudulent install on contributor1
    SELECT '2022-03-29 18:08:50 UTC', 'install', 'fraudcpa', '2022-03-29 15:00:50 UTC', 'goodcpa', null, null, null, null, '100-200', 'jjj100-kkk100'
  ),
  post_attribution_installs AS (
    SELECT 
      '777-888' AS appsflyer_id,
       NULL AS rejected_reason_value
  UNION ALL
    SELECT '999-100', 'organic'
  UNION ALL
    SELECT '100-200', 'contributor1'
  ),
  inapps AS (
    SELECT 
      '2022-03-31 18:02:47 UTC' AS install_time,
      'content_view' AS event_name, 
      '2022-03-31 18:06:02 UTC' AS event_time, 
      'googleads' AS media_source,
      '111-222' AS appsflyer_id,
      '111' AS customer_user_id
  UNION ALL
    SELECT '2022-03-31 18:02:47 UTC', 'registation', '2022-03-31 18:09:47 UTC', 'googleads', '111-222', '111'
  UNION ALL
    SELECT '2022-03-30 18:05:47 UTC', 'content_view', '2022-03-30 18:07:47 UTC', 'organic', '333-444', '333'
  UNION ALL
    SELECT '2022-03-29 17:06:47 UTC', 'content_view', '2022-03-29 17:08:50 UTC', 'facebook', '555-666', null),
  clicks_retargeting AS (
    SELECT
        '2022-04-01 21:03:02 UTC' AS event_time,
        'click' AS event_name,
        'email' AS media_source,
        'aaa111-bbb111' AS advertising_id
    UNION ALL
     SELECT '2022-03-31 08:15:23 UTC', 'click', 'google_retarg', 'bbb222-ccc222'
  ),
  inapps_retargeting AS (
    SELECT
      'email' AS media_source,
      '2022-04-01 19:06:02 UTC' AS event_time, 
      'content_view' AS event_name, 
      '111-222' AS appsflyer_id,
      '111' AS customer_user_id
  UNION ALL
    SELECT 'email', '2022-04-01 20:07:02 UTC', 'login', '111-222', '111'
  UNION ALL
    SELECT 'google_retarg', '2022-03-31 09:07:02 UTC', 'registration', '333-444', '333'
  UNION ALL
    SELECT 'cpanetwork_retarg', '2022-03-31 10:10:10 UTC', 'content_view', '100-200', NULL
  UNION ALL
    SELECT 'cpanetwork_retarg', '2022-03-31 13:15:10 UTC', 'content_view', '999-100', NULL
  ),
  post_attribution_in_app_events AS (
    SELECT
      '2022-03-31 10:10:10 UTC' AS event_time, 
      'content_view' AS event_name,
      '100-200' AS appsflyer_id,
      TRUE AS is_retargeting,
      'organic' AS rejected_reason_value
  UNION ALL
    SELECT '2022-03-31 13:15:10 UTC', 'content_view', '999-100', TRUE, NULL
  ),
  organic_uninstalls AS (
    SELECT
      'googleads' AS media_source,
      '2022-04-01 10:09:47 UTC' AS event_time,
      '333-444' AS appsflyer_id
  ),
  uninstalls AS (
    SELECT
      'googleads' AS media_source,
      '2022-04-02 18:09:47 UTC' AS event_time,
      '111-222' AS appsflyer_id
  ),
  raw_data AS
    (SELECT
        customer_user_id,
        CASE
            WHEN true_install_time_by_userid IS NULL THEN true_install_time_by_afid
            ELSE true_install_time_by_afid
        END AS true_install_time,
        appsflyer_id,
        media_source,
        true_install_source,
        clicks_media_source,
        clicks_event_name,
        clicks_event_time,
        install_time,
        contributor_1_media_source,
        contributor_1_touch_time,
        contributor_2_media_source,
        contributor_2_touch_time,
        contributor_3_media_source,
        contributor_3_touch_time,
        clicks_retarg_source,
        clicks_retarg_event_name,
        clicks_retarg_event_time,
        retarg_source,
        retarg_event_name,
        retarg_event_time,
        inapps_media_source,
        inapps_event_name,
        inapps_event_time,
        uninstall_organic_source,
        uninstall_organic_time,
        uninstall_nonorg_source,
        uninstall_nonorg_time
    FROM
        (SELECT
            customer_user_id,
            FIRST_VALUE(install_time) OVER (PARTITION BY customer_user_id ORDER BY install_time ASC) AS true_install_time_by_userid,
            FIRST_VALUE(install_time) OVER (PARTITION BY appsflyer_id ORDER BY install_time ASC) AS true_install_time_by_afid,
            appsflyer_id,
            media_source,
            true_install_source,
            clicks_media_source,
            clicks_event_name,
            clicks_event_time,
            install_time,
            contributor_1_media_source,
            contributor_1_touch_time,
            contributor_2_media_source,
            contributor_2_touch_time,
            contributor_3_media_source,
            contributor_3_touch_time,
            clicks_retarg_source,
            clicks_retarg_event_name,
            clicks_retarg_event_time,
            retarg_source,
            retarg_event_name,
            retarg_event_time,
            inapps_media_source,
            inapps_event_name,
            inapps_event_time,
            uninstall_organic_source,
            uninstall_organic_time,
            uninstall_nonorg_source,
            uninstall_nonorg_time
        FROM
            (SELECT
                LAST_VALUE(customer_user_id IGNORE NULLS) OVER (PARTITION BY appsflyer_id ORDER BY install_time DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_user_id,
                appsflyer_id,
                media_source,
                true_install_source,
                clicks_media_source,
                clicks_event_name,
                clicks_event_time,
                install_time,
                contributor_1_media_source,
                contributor_1_touch_time,
                contributor_2_media_source,
                contributor_2_touch_time,
                contributor_3_media_source,
                contributor_3_touch_time,
                clicks_retarg_source,
                clicks_retarg_event_name,
                clicks_retarg_event_time,
                retarg_source,
                retarg_event_name,
                retarg_event_time,
                inapps_media_source,
                inapps_event_name,
                inapps_event_time,
                uninstall_organic_source,
                uninstall_organic_time,
                uninstall_nonorg_source,
                uninstall_nonorg_time
            FROM
                (SELECT
                    DISTINCT 
                    customer_user_id,
                    --delete fraud installs
                    CASE 
                        WHEN problem_install_afid IS NOT NULL AND true_install_source IS NULL THEN 'delete'
                        ELSE appsflyer_id
                    END AS appsflyer_id,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'organic' THEN 'organic'
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor1' THEN contributor_1_media_source
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor2' THEN contributor_2_media_source
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor3' THEN contributor_3_media_source
                        ELSE media_source
                    END AS media_source,
                    true_install_source,
                    clicks_media_source,
                    clicks_event_name,
                    clicks_event_time,
                    CASE
                        WHEN install_time_inapp IS NULL THEN install_time_installs
                        ELSE install_time_inapp
                    END AS install_time,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor1' THEN NULL
                        ELSE contributor_1_media_source
                    END AS contributor_1_media_source,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor1' THEN NULL
                        ELSE contributor_1_touch_time
                    END AS contributor_1_touch_time,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor2' THEN NULL
                        ELSE contributor_2_media_source
                    END AS contributor_2_media_source,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor2' THEN NULL
                        ELSE contributor_2_touch_time
                    END AS contributor_2_touch_time,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor3' THEN NULL
                        ELSE contributor_3_media_source
                    END AS contributor_3_media_source,
                    CASE
                        WHEN problem_install_afid IS NOT NULL AND true_install_source = 'contributor3' THEN NULL
                        ELSE contributor_3_touch_time
                    END AS contributor_3_touch_time,
                    clicks_retarg_source,
                    clicks_retarg_event_name,
                    clicks_retarg_event_time,
                    --make nulls in retargeting columns if fraud
                    CASE 
                        WHEN true_retarg_source = 'delete' THEN NULL
                        WHEN true_retarg_source IS NOT NULL AND true_retarg_source != 'delete' THEN true_retarg_source
                        ELSE retarg_source
                    END AS retarg_source,
                    CASE 
                        WHEN true_retarg_source = 'delete' THEN NULL
                        ELSE retarg_event_name
                    END AS retarg_event_name,
                    CASE 
                        WHEN true_retarg_source = 'delete' THEN NULL
                        ELSE retarg_event_time
                    END AS retarg_event_time,
                    inapps_media_source,
                    inapps_event_name,
                    inapps_event_time,
                    uninstall_organic_source,
                    uninstall_organic_time,
                    uninstall_nonorg_source,
                    uninstall_nonorg_time
                FROM
                    (SELECT
                        inapps.customer_user_id,
                        installs.appsflyer_id,
                        installs.media_source,
                        post_attribution_installs.appsflyer_id AS problem_install_afid,
                        rejected_reason_value AS true_install_source,
                        clicks.media_source AS clicks_media_source,
                        clicks.event_name AS clicks_event_name,
                        CAST(clicks.event_time AS STRING) AS clicks_event_time,
                        CAST(installs.install_time AS STRING) AS install_time_installs,
                        CAST(inapps.install_time AS STRING) AS install_time_inapp,
                        installs.contributor_1_media_source,
                        CAST(installs.contributor_1_touch_time AS STRING) AS contributor_1_touch_time,
                        installs.contributor_2_media_source,
                        CAST(installs.contributor_2_touch_time AS STRING) AS contributor_2_touch_time,
                        installs.contributor_3_media_source,
                        CAST(installs.contributor_3_touch_time AS STRING) AS contributor_3_touch_time,
                        clicks_retarg.media_source AS clicks_retarg_source,
                        clicks_retarg.event_name AS clicks_retarg_event_name,
                        CAST(clicks_retarg.event_time AS STRING) AS clicks_retarg_event_time,
                        retarg.media_source AS retarg_source,
                        retarg.event_name AS retarg_event_name,
                        CAST(retarg.event_time AS STRING) AS retarg_event_time,
                        fraud_retarg.media_source AS true_retarg_source,
                        inapps.media_source AS inapps_media_source,
                        inapps.event_name AS inapps_event_name,
                        CAST(inapps.event_time AS STRING) AS inapps_event_time,
                        organic_uninstalls.media_source AS uninstall_organic_source,
                        CAST(organic_uninstalls.event_time AS STRING) AS uninstall_organic_time,
                        uninstalls.media_source AS uninstall_nonorg_source,
                        CAST(uninstalls.event_time AS STRING) AS uninstall_nonorg_time
                    FROM
                        (SELECT
                            appsflyer_id,
                            advertising_id,
                            media_source,
                            install_time,
                            contributor_1_media_source,
                            contributor_1_touch_time,
                            contributor_2_media_source,
                            contributor_2_touch_time,
                            contributor_3_media_source,
                            contributor_3_touch_time
                        FROM
                            installs
                            ) AS installs
                        LEFT JOIN
                        (SELECT
                            advertising_id,
                            media_source,
                            event_name,
                            event_time
                        FROM clicks
                            ) AS clicks
                        ON installs.advertising_id = clicks.advertising_id
                        LEFT JOIN
                        (SELECT
                            appsflyer_id,
                            rejected_reason_value
                        FROM post_attribution_installs
                        ) AS post_attribution_installs
                        ON installs.appsflyer_id = post_attribution_installs.appsflyer_id
                        LEFT JOIN --add customer_user_id, because all customer_user_id are NULL at installs
                        (SELECT
                            customer_user_id,
                            appsflyer_id,
                            media_source,
                            install_time,
                            event_name,
                            event_time
                        FROM
                            inapps
                        ) AS inapps
                        ON installs.appsflyer_id = inapps.appsflyer_id
                        LEFT JOIN
                        (SELECT
                            advertising_id,
                            media_source,
                            event_name,
                            event_time
                        FROM 
                            clicks_retargeting
                        ) AS clicks_retarg
                        ON installs.advertising_id = clicks_retarg.advertising_id
                        LEFT JOIN --add retarteting campaigns and events name with event_time (as contidition)
                        (SELECT
                            customer_user_id,
                            appsflyer_id,
                            CONCAT('utm_retargeting=', media_source) AS media_source,
                            event_time,
                            event_name
                        FROM
                            inapps_retargeting
                        ) AS retarg
                        ON installs.appsflyer_id = retarg.appsflyer_id
                        LEFT JOIN
                        (SELECT
                            appsflyer_id,
                            CASE
                                WHEN rejected_reason_value IS NULL THEN 'delete'
                                ELSE CONCAT('utm_retargeting=', rejected_reason_value)
                            END AS media_source,
                            event_time,
                            event_name
                        FROM
                            post_attribution_in_app_events
                        WHERE is_retargeting IS TRUE
                        ) AS fraud_retarg
                        ON retarg.appsflyer_id = fraud_retarg.appsflyer_id 
                        AND retarg.event_time = fraud_retarg.event_time
                        AND retarg.event_name = fraud_retarg.event_name
                        LEFT JOIN
                        (SELECT
                            appsflyer_id,
                            IF(media_source IS NULL, 'none', 'none') AS media_source,
                            event_time
                        FROM
                            organic_uninstalls
                        ) AS organic_uninstalls
                        ON installs.appsflyer_id = organic_uninstalls.appsflyer_id
                        LEFT JOIN
                        (SELECT
                            appsflyer_id,
                            IF(media_source IS NULL, 'none', 'none') AS media_source,
                            event_time
                        FROM
                            uninstalls
                        ) AS uninstalls
                        ON installs.appsflyer_id = uninstalls.appsflyer_id
                    )
                )
            )
            --excluding user with adif = '777-888' AND
            WHERE (appsflyer_id != 'delete')
        )
    ),
    channels AS 
    (SELECT
        DISTINCT 
        appsflyer_id,
        customer_user_id,
        CASE 
            WHEN channel LIKE '%utm_retargeting%' THEN REGEXP_EXTRACT(channel, 'utm_retargeting=([^&]*)')
            ELSE channel
        END AS channel,
        CASE
            WHEN channel LIKE '%utm_retargeting%' THEN 1
            ELSE 0
        END AS is_retarg,
        true_install_time,
        CAST(touch_time AS TIMESTAMP) AS touch_time
    FROM
        raw_data
    UNPIVOT ((channel, touch_time)  FOR  channel_time_pair IN
            ((clicks_media_source, clicks_event_time),
            (media_source, install_time), 
            (contributor_1_media_source, contributor_1_touch_time), 
            (contributor_2_media_source, contributor_2_touch_time),
            (contributor_3_media_source, contributor_3_touch_time), 
            (clicks_retarg_source, clicks_retarg_event_time),
            (inapps_media_source, inapps_event_time),
            (retarg_source, retarg_event_time),
            (uninstall_organic_source, uninstall_organic_time),
            (uninstall_nonorg_source, uninstall_nonorg_time)
            ))
    ),
    events AS
    (SELECT
        DISTINCT 
        appsflyer_id,
        customer_user_id,
        event,
        CAST(time_of_event AS TIMESTAMP) AS time_of_event
    FROM
        raw_data
    UNPIVOT ((event, time_of_event)  FOR  event_time_pair IN
            ((clicks_event_name, clicks_event_time),
            (media_source, install_time), 
            (clicks_retarg_event_name, clicks_retarg_event_time),
            (inapps_event_name, inapps_event_time),
            (retarg_event_name, retarg_event_time)
            ))
    ),
    user_journey AS 
    (SELECT
        appsflyer_id,
        customer_user_id,
        CASE
            WHEN LAG(channel) OVER (PARTITION BY appsflyer_id, customer_user_id, is_retarg ORDER BY touch_time ASC ) = channel 
                AND event_name = 'install' THEN channel
            WHEN LAG(channel) OVER (PARTITION BY appsflyer_id, customer_user_id, is_retarg ORDER BY touch_time ASC ) = channel
                AND event_name IS NOT NULL THEN 'direct/none'
            ELSE channel
        END AS channel,
        touch_time,
        --or leave it as null if you wish
        --event_name
        CASE WHEN
            event_name IS NULL THEN 'click'
            ELSE event_name
        END AS event_name
        FROM(
            SELECT
                appsflyer_id,
                customer_user_id,
                channel,
                is_retarg,
                touch_time,
                CASE 
                    --where can not be events before install!
                    WHEN CAST(true_install_time AS TIMESTAMP) > CAST(touch_time AS TIMESTAMP) THEN NULL
                    --install event
                    WHEN CAST(true_install_time AS TIMESTAMP) = touch_time THEN 'install'
                    WHEN channel = 'none' THEN 'uninstall'
                    WHEN event = 'click' THEN NULL
                    ELSE event
                END AS event_name
            FROM
                (SELECT
                    channels.appsflyer_id,
                    channels.customer_user_id,
                    channel,
                    is_retarg,
                    true_install_time,
                    touch_time,
                    event
                FROM
                    (SELECT
                        *
                    FROM channels) AS channels
                    LEFT JOIN
                (SELECT
                    *
            FROM events) AS events
            ON channels.appsflyer_id = events.appsflyer_id
            AND channels.customer_user_id = events.customer_user_id
            AND channels.touch_time = events.time_of_event
            ORDER BY channels.appsflyer_id, touch_time)
        )
    ),
    conversion_paths_by_user AS    
    (SELECT
        appsflyer_id,
        customer_user_id,
        channel,
        event_name
    FROM
        (SELECT
            *,
            --MIN = first interaction with desired event_name
            MIN(CASE WHEN event_name = 'install' THEN touch_number END) OVER (PARTITION BY appsflyer_id, customer_user_id) AS conversion
        FROM
            (SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY appsflyer_id, customer_user_id) AS touch_number
            FROM user_journey)
        )
    WHERE touch_number <= conversion
    ),
    multi_channel_funnel AS (
    SELECT
        multi_channel_funnel,
        COUNT(multi_channel_funnel) AS count
    FROM
        (SELECT
            ARRAY_TO_STRING(multi_channel_funnel, '->') AS multi_channel_funnel
        FROM
            (SELECT
                appsflyer_id,
                customer_user_id,
                ARRAY_AGG(channel) AS multi_channel_funnel
            FROM conversion_paths_by_user
            GROUP BY 1, 2)
        )
    GROUP BY 1)

SELECT
    *
FROM multi_channel_funnel

/*
SELECT
    *
FROM user_journey
ORDER BY appsflyer_id, customer_user_id, touch_time ASC
*/

/*
--raw data check
SELECT
*
FROM raw_data
--ORDER BY customer_user_id ASC
*/
```sql
