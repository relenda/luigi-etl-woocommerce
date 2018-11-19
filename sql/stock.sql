SELECT
  m_stock.post_id                     AS product_id,
  CAST(m_stock.meta_value AS SIGNED)  AS stock,
  (
    SELECT CAST(m_purchased.meta_value AS SIGNED)
    FROM wpshop_postmeta m_purchased
    WHERE m_purchased.meta_key = '_original_stock' AND m_purchased.post_id = m_stock.post_id
    UNION
    SELECT 0
    LIMIT 1
  )                                   AS purchased_stock
FROM wpshop_postmeta m_stock
  INNER JOIN wpshop_posts product
    ON product.ID = m_stock.post_id AND product.post_type IN ('product_variation')
  -- assure that we do not export dangling records
  INNER JOIN wpshop_posts parent
    ON product.post_parent = parent.ID
WHERE 1
      AND m_stock.meta_key = '_stock'
      AND product.ID > 7294 -- exclude corrupted old records @see products.sql
      AND parent.ID NOT IN (7161, 32942, 18551)  -- exclude Gutscheine, kostenlose Rücksendescheine, zusätzliche Rücksendescheine
      AND parent.post_status = 'publish'
;