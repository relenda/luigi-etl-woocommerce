/*
duplicates are resolved on UNION level

generation of address_ids is based on
http://greenash.net.au/thoughts/2010/03/generating-unique-integer-ids-from-strings-in-mysql/
*/
SELECT
  'billing'                    AS address_type,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_customer_user' AND post_id = ordr.ID
  )                            AS customer_id,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_address_1' AND post_id = ordr.ID
  )                            AS address1,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_address_2' AND post_id = ordr.ID
  )                            AS address2,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_company' AND post_id = ordr.ID
  )                            AS company,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_first_name' AND post_id = ordr.ID
  )                            AS firstname,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_last_name' AND post_id = ordr.ID
  )                            AS lastname,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_postcode' AND post_id = ordr.ID
  )                            AS zipcode,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_city' AND post_id = ordr.ID
  )                            AS city,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_state' AND post_id = ordr.ID
  )                            AS state,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_billing_country' AND post_id = ordr.ID
  )                            AS country,

  /* @formatter:off */
  -- DO NEVER TOUCH (order, letter case, and attributes matter) any changes break a lot
  -- datawarehouse/lib/address_hash.sql
  CONV(SUBSTRING(CAST(SHA(
  CONCAT_WS(',',
      (select customer_id), (select address1),     (select address2),
      (select company),     (select firstname),    (select lastname),
      (select zipcode),     (select city),         (select state),
      (select country),     (select address_type)
  )
  ) AS CHAR), 1, 16), 16, 10)
  AS address_id
  /* @formatter:on*/
FROM
  wpshop_posts ordr
WHERE
  ordr.post_type = 'shop_order'
  AND ordr.post_status = 'wc-completed'
  AND ordr.post_date >= '2014-09-01'
UNION
SELECT
  'shipping' AS address_type,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_customer_user' AND post_id = ordr.ID
  )          AS customer_id,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_address_1' AND post_id = ordr.ID
  )          AS address1,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_address_2' AND post_id = ordr.ID
  )          AS address2,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_company' AND post_id = ordr.ID
  )          AS company,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_first_name' AND post_id = ordr.ID
  )          AS firstname,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_last_name' AND post_id = ordr.ID
  )          AS lastname,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_postcode' AND post_id = ordr.ID
  )          AS zipcode,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_city' AND post_id = ordr.ID
  )          AS city,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_state' AND post_id = ordr.ID
  )          AS state,
  (
    SELECT meta_value
    FROM wpshop_postmeta
    WHERE meta_key = '_shipping_country' AND post_id = ordr.ID
  )          AS country,

  /* @formatter:off */
  -- DO NEVER TOUCH (order, letter case, and attributes matter) any changes break a lot
  -- datawarehouse/lib/address_hash.sql
  CONV(SUBSTRING(CAST(SHA(
  CONCAT_WS(',',
      (select customer_id), (select address1),    (select address2),
      (select company),     (select firstname),   (select lastname),
      (select zipcode),     (select city),        (select state),
      (select country),     (select address_type)
  )
  ) AS CHAR), 1, 16), 16, 10)
  AS address_id
  /* @formatter:on*/
FROM
  wpshop_posts ordr
WHERE
  ordr.post_type = 'shop_order'
  AND ordr.post_status = 'wc-completed'
  AND ordr.post_date >= '2014-09-01';