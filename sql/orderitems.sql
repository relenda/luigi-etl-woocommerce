SELECT
  item.order_item_id                                             AS order_item_id,
  item.order_id                                                  AS order_id,
  REPLACE(m_delivered.meta_key, '_delivered_', '')               AS subqty_id,

  -- shipping details
  CAST(FROM_UNIXTIME(m_delivered.meta_value) AS DATE)            AS delivered_on,
  (
    SELECT CAST(FROM_UNIXTIME(meta_value) AS DATE)
    FROM wpshop_woocommerce_order_itemmeta
    WHERE order_item_id = item.order_item_id AND
          meta_key = CONCAT('_returned_', subqty_id)
  )                                                              AS returned_on,
  IF((SELECT delivered_on) >= (SELECT returned_on), TRUE, FALSE) AS is_retoure,

  -- product details
  (
    SELECT CASE
           WHEN m_variation.meta_value >= 7297
             THEN m_variation.meta_value
           WHEN m_product.meta_value >= 7297
             THEN m_product.meta_value
           ELSE 7297 #FIXME: this needs dummy product
           END
    FROM wpshop_woocommerce_order_itemmeta m_product
      LEFT JOIN wpshop_woocommerce_order_itemmeta m_variation USING (order_item_id)
    WHERE 1
          AND m_product.order_item_id = item.order_item_id
          AND m_product.meta_key = '_product_id'
          AND m_variation.meta_key = '_variation_id'
          AND (m_variation.meta_value >= 7297 OR m_product.meta_value >= 7297)
          AND (m_variation.meta_value NOT IN (9666, 9921, 9922, 13925, 13927, 14170, 18551, 18660, 32942) AND
               m_product.meta_value NOT IN (9666, 9921, 9922, 13925, 13927, 14170, 18551, 18660, 32942))
    UNION -- this prevents NULL results if neither m_variation nor m_product matches
      SELECT 7294
    LIMIT 1

  )                                                              AS product_id,
  item.order_item_name                                           AS product_name,

  -- price
  (
    SELECT
      /* @formatter:off */
      CAST(
        CAST(m_line_net.meta_value AS DECIMAL(10, 2))
      / CAST(m_qty.meta_value AS DECIMAL(10, 2))
      AS DECIMAL(10, 2))
      /* @formatter:on*/
    FROM wpshop_woocommerce_order_itemmeta m_line_net
      INNER JOIN wpshop_woocommerce_order_itemmeta m_qty USING (order_item_id)
    WHERE
      m_line_net.order_item_id = item.order_item_id AND
      m_line_net.meta_key = '_line_subtotal' AND
      m_qty.meta_key = '_qty'

  )                                                              AS price_net,
  (
    SELECT
      /* @formatter:off */
      CAST(
        CAST(m_line_tax.meta_value AS DECIMAL(10, 2))
      / CAST(m_qty.meta_value AS DECIMAL(10, 2))
      AS DECIMAL(10, 2))
      /* @formatter:on*/
    FROM wpshop_woocommerce_order_itemmeta m_line_tax
      INNER JOIN wpshop_woocommerce_order_itemmeta m_qty USING (order_item_id)
    WHERE
      m_line_tax.order_item_id = item.order_item_id AND
      m_line_tax.meta_key = '_line_subtotal_tax' AND
      m_qty.meta_key = '_qty'

  )                                                              AS price_tax,

  -- discount
  (
    SELECT
      /* @formatter:off */
      CAST(
          (
            CAST(m_line_undiscounted_net.meta_value AS DECIMAL(10, 2))
          - CAST(m_line_discounted_net.meta_value AS DECIMAL(10, 2))
          )
          / CAST(m_qty.meta_value AS DECIMAL(10, 2))
      AS DECIMAL(10, 2))
      /* @formatter:on*/
    FROM wpshop_woocommerce_order_itemmeta m_line_discounted_net
      INNER JOIN wpshop_woocommerce_order_itemmeta m_line_undiscounted_net USING (order_item_id)
      INNER JOIN wpshop_woocommerce_order_itemmeta m_qty USING (order_item_id)
    WHERE
      m_line_discounted_net.order_item_id = item.order_item_id AND
      m_line_discounted_net.meta_key = '_line_subtotal' AND
      m_line_undiscounted_net.meta_key = '_line_total' AND
      m_qty.meta_key = '_qty'

  )                                                              AS discount_net,
  (
    SELECT
      /* @formatter:off */
      CAST(
          (
            CAST(m_line_undiscounted_tax.meta_value AS DECIMAL(10, 2))
          - CAST(m_line_discounted_tax.meta_value AS DECIMAL(10, 2))
          )
          / CAST(m_qty.meta_value AS DECIMAL(10, 2))
      AS DECIMAL(10, 2))
      /* @formatter:on*/
    FROM wpshop_woocommerce_order_itemmeta m_line_discounted_tax
      INNER JOIN wpshop_woocommerce_order_itemmeta m_line_undiscounted_tax USING (order_item_id)
      INNER JOIN wpshop_woocommerce_order_itemmeta m_qty USING (order_item_id)
    WHERE
      m_line_discounted_tax.order_item_id = item.order_item_id AND
      m_line_discounted_tax.meta_key = '_line_subtotal_tax' AND
      m_line_undiscounted_tax.meta_key = '_line_tax' AND
      m_qty.meta_key = '_qty'

  )                                                              AS discount_tax,

  -- payment
  (SELECT price_net) + (SELECT discount_net)                     AS payment_net,
  (SELECT price_tax) + (SELECT discount_tax)                     AS payment_tax
FROM wpshop_woocommerce_order_items item
  INNER JOIN wpshop_woocommerce_order_itemmeta m_delivered USING (order_item_id)
  INNER JOIN wpshop_posts ordr
    ON ordr.id = item.order_id AND ordr.post_status = 'wc-completed' AND ordr.post_date > '2014-09-01'
WHERE
  m_delivered.meta_key LIKE '_delivered_%';