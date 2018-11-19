CREATE TABLE IF NOT EXISTS products__last_order_date (
  product_id serial,
  last_order_at timestamp
);

TRUNCATE products__last_order_date;

INSERT INTO products__last_order_date (last_order_at, product_id)

SELECT MAX(p.post_date), itemmeta.meta_value
FROM wpshop_woocommerce_order_itemmeta as itemmeta
  INNER JOIN wpshop_woocommerce_order_items as items
    ON itemmeta.order_item_id = items.order_item_id
  INNER JOIN wpshop_posts as p
    ON p.ID = items.order_id
WHERE itemmeta.meta_key = '_variation_id'
  AND p.post_status = 'wc-completed'
GROUP BY itemmeta.meta_value;