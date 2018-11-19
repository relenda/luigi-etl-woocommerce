SELECT
  ordr.ID,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_customer_user' AND
          post_id = ordr.ID
  )                                         AS customer_id,
  ordr.post_date                            AS order_date,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_completed_date' AND
          post_id = ordr.ID
  )                                         AS order_completed_date,
  (
    -- result can be <null> as in no result or '' as in cell is empty string
    SELECT CASE
           WHEN LENGTH(meta_value) = 0 OR meta_value IS NULL
             THEN 'NONE'
           ELSE meta_value
           END
    FROM wpshop_postmeta
    WHERE meta_key = '_payment_method' AND
          post_id = ordr.ID
    UNION
    SELECT 'NONE'
    LIMIT 1
  )                                         AS payment_method,
  -- order totals
  (
    SELECT CAST(meta_value AS DECIMAL(10, 2))
    FROM wpshop_postmeta
    WHERE meta_key = '_order_total' AND
          post_id = ordr.ID
  )                                         AS order_total,
  (
    SELECT CAST(ROUND(meta_value, 2) AS DECIMAL(10, 2))
    FROM wpshop_postmeta
    WHERE meta_key = '_order_tax' AND
          post_id = ordr.ID
  )                                         AS order_tax,
  (SELECT order_total) - (SELECT order_tax) AS order_net,
  -- cart discounts
  (
    SELECT CAST(meta_value AS DECIMAL(10, 2))
    FROM wpshop_postmeta
    WHERE meta_key = '_cart_discount' AND
          post_id = ordr.ID
  )                                         AS discount_net,
  (
    SELECT CAST(ROUND(meta_value, 2) AS DECIMAL(10, 2))
    FROM wpshop_postmeta
    WHERE meta_key = '_cart_discount_tax' AND
          post_id = ordr.ID
  )                                         AS discount_tax,
  -- subtotals
  (
    SELECT CAST(SUM(CAST(meta_value AS DECIMAL(10, 4))) AS DECIMAL(10, 2))
    FROM wpshop_woocommerce_order_items items
      INNER JOIN wpshop_woocommerce_order_itemmeta meta
        ON meta.order_item_id = items.order_item_id AND
           meta.meta_key = '_line_subtotal'
    WHERE items.order_id = ordr.ID
  )                                         AS subtotal,
  (
    SELECT CAST(SUM(CAST(meta_value AS DECIMAL(10, 4))) AS DECIMAL(10, 2))
    FROM wpshop_woocommerce_order_items items
      INNER JOIN wpshop_woocommerce_order_itemmeta meta
        ON meta.order_item_id = items.order_item_id AND
           meta.meta_key = '_line_subtotal_tax'
    WHERE items.order_id = ordr.ID
  )                                         AS subtotal_tax,
  (SELECT subtotal) - (SELECT subtotal_tax) AS subtotal_net,

  -- billing and shipping address
  -- @see datawarehouse/lib/address_hash.sql
  -- billing_address
  (
    SELECT
      /* @formatter:off */
      CONV(SUBSTRING(CAST(SHA(
      CONCAT(GROUP_CONCAT(
          meta_value
          ORDER BY
          FIELD(meta_key, '_customer_user', '_billing_address_1', '_billing_address_2',
                '_billing_company', '_billing_first_name', '_billing_last_name',
                '_billing_postcode', '_billing_city', '_billing_state', '_billing_country')
          SEPARATOR ','), ',billing')
        ) AS CHAR), 1, 16), 16, 10)
        AS address_id
      /* @formatter:on*/
    FROM wpshop_postmeta
    WHERE post_id = ordr.ID AND
          meta_key IN ('_customer_user', '_billing_address_1', '_billing_address_2',
                       '_billing_company', '_billing_first_name', '_billing_last_name',
                       '_billing_postcode', '_billing_city', '_billing_state', '_billing_country')
    GROUP BY post_id
  )                                         AS billing_address,

  -- shipping_address
  (
    SELECT
      /* @formatter:off */
      CONV(SUBSTRING(CAST(SHA(
      CONCAT(GROUP_CONCAT(
          meta_value
          ORDER BY
          FIELD(meta_key, '_customer_user', '_shipping_address_1', '_shipping_address_2',
                '_shipping_company', '_shipping_first_name', '_shipping_last_name',
                '_shipping_postcode', '_shipping_city', '_shipping_state', '_shipping_country')
          SEPARATOR ','), ',shipping')
        ) AS CHAR), 1, 16), 16, 10)
        AS address_id
      /* @formatter:on*/
    FROM wpshop_postmeta
    WHERE post_id = ordr.ID AND
          meta_key IN ('_customer_user', '_shipping_address_1', '_shipping_address_2',
                       '_shipping_company', '_shipping_first_name', '_shipping_last_name',
                       '_shipping_postcode', '_shipping_city', '_shipping_state', '_shipping_country')
    GROUP BY post_id
  )                                         AS shipping_address,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_last_contact_campaign_id' AND
          post_id = ordr.ID
  )                                      AS last_contact_campaign_id,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_first_contact_campaign_id' AND
          post_id = ordr.ID
  )                                      AS first_contact_campaign_id,

    (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_last_contact_campaign_recorded_at' AND
          post_id = ordr.ID
  )                                      AS last_contact_campaign_recorded_at,

  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_first_contact_campaign_recorded_at' AND
          post_id = ordr.ID
  )                                      AS first_contact_campaign_recorded_at

FROM wpshop_posts ordr
WHERE
  ordr.post_type = 'shop_order'
  AND ordr.post_status = 'wc-completed'
  AND ordr.post_date >= '2014-09-01'
ORDER BY ordr.ID ASC;