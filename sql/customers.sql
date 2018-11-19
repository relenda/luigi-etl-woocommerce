SELECT
  users.ID          AS id,
  (
    SELECT meta_value
    FROM wpshop_usermeta um
    WHERE um.user_id = users.ID
          AND um.meta_key = 'billing_first_name'
  )                 AS firstname,
  (
    SELECT meta_value
    FROM wpshop_usermeta um
    WHERE um.user_id = users.ID AND
          um.meta_key = 'billing_last_name'
  )                 AS lastname,
  (
    SELECT meta_value
    FROM wpshop_usermeta um
    WHERE um.user_id = users.ID AND
          um.meta_key = 'billing_phone'
  )                 AS phone,
  users.user_email  AS email,
  user_registered   AS registered_date,
  first_order_id.ID AS first_order_id,
  (
    SELECT post_date
    FROM wpshop_posts ordr
    WHERE ordr.ID = first_order_id.ID AND
          ordr.post_status = 'wc-completed'
  )                 AS first_order_date,
  last_order_id.ID AS last_order_id,
  (
    SELECT post_date
    FROM wpshop_posts ordr
    WHERE ordr.ID = last_order_id.ID AND
          ordr.post_status = 'wc-completed'
  )                 AS last_order_date,
  (
    SELECT meta_value
    FROM wpshop_postmeta origin
    WHERE meta_key = 'customer_origin' AND
          post_id = first_order_id.ID
  )                 AS origin,
  1                 AS last_billing_address
FROM wpshop_users users
  INNER JOIN
  (
    -- this is for performance reasons a join
    SELECT
      min(post_id) AS ID,
      meta_value   AS customer_id
    FROM wpshop_postmeta AS ordermeta
      INNER JOIN wpshop_posts AS orders ON orders.id = ordermeta.post_id
    WHERE
      meta_key = '_customer_user'
      AND orders.post_status = 'wc-completed'
      AND orders.post_date >= '2014-09-01'
    GROUP BY meta_value
  ) first_order_id ON first_order_id.customer_id = users.ID
  INNER JOIN
  (
    -- this is for performance reasons a join
    SELECT
      max(post_id) AS ID,
      meta_value   AS customer_id
    FROM wpshop_postmeta AS ordermeta
      INNER JOIN wpshop_posts AS orders ON orders.id = ordermeta.post_id
    WHERE
      meta_key = '_customer_user'
      AND orders.post_status = 'wc-completed'
      AND orders.post_date >= '2014-09-01'
    GROUP BY meta_value
  ) last_order_id ON last_order_id.customer_id = users.ID
ORDER BY users.ID ASC;