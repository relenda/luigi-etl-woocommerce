ALTER TABLE customers
  ADD COLUMN last_order_id int;

ALTER TABLE customers
  ADD COLUMN last_order_date timestamp;

ALTER TABLE products
  ADD COLUMN last_order_at timestamp;
