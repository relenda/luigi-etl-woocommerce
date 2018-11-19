CREATE TABLE IF NOT EXISTS addresses (
  type varchar(255),
  customer_id int,
  address1 text,
  address2 text,
  company varchar(255),
  firstname varchar(255),
  lastname varchar(255),
  zipcode varchar(255),
  city varchar(255),
  state varchar(255),
  country varchar(255),
  id numeric(20),
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS orders (
  id serial,
  customer_id int,
  order_date timestamp,
  order_completed_date timestamp,
  payment_method varchar(255),
  order_total numeric(8, 2),
  order_tax numeric(8, 2),
  order_net numeric(8, 2),
  discount_net numeric(8, 2),
  discount_tax numeric(8, 2),
  subtotal numeric(8, 2),
  subtotal_tax numeric(8, 2),
  subtotal_net numeric(8, 2),
  billig_address_id numeric(20),
  shipping_address_id numeric(20),
  PRIMARY KEY (id),
  FOREIGN KEY (billig_address_id) REFERENCES addresses(id) DEFERRABLE INITIALLY IMMEDIATE,
  FOREIGN KEY (shipping_address_id) REFERENCES addresses(id) DEFERRABLE INITIALLY IMMEDIATE
);
CREATE TABLE IF NOT EXISTS customers (
  id serial,
  firstname varchar(255),
  lastname varchar(255),
  phone varchar(255),
  email varchar(255),
  registered_date timestamp,
  first_order_id int,
  first_order_date timestamp,
  origin varchar(255),
  last_billing_address_id numeric(20),
  PRIMARY KEY (id),
  FOREIGN KEY (first_order_id) REFERENCES orders(id) DEFERRABLE INITIALLY IMMEDIATE,
  FOREIGN KEY (last_billing_address_id) REFERENCES addresses(id) DEFERRABLE INITIALLY IMMEDIATE
);
ALTER TABLE addresses ADD FOREIGN KEY (customer_id) REFERENCES customers(id) DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE IF NOT EXISTS products (
  id serial,
  name varchar(255),
  parent_id int,
  thumbnail_id int,
  primary_category varchar(255),
  super_category varchar(255),
  uvp numeric(8, 2),
  cost_of_goods numeric(8, 2),
  color varchar(255),
  brand varchar(255),
  order_season varchar(255),
  season_fruehling boolean,
  season_sommer boolean,
  season_herbst boolean,
  season_winter boolean,
  size varchar(255),
  size_shopfilter varchar(255),
  item_condition varchar(255),
  created_at timestamp,
  updated_at timestamp,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS orders_products (
  order_item_id int,
  order_id int,
  subqty_id int,
  delivered_on timestamp,
  returned_on timestamp,
  is_retoure boolean,
  product_id int,
  product_name varchar(255),
  price_net numeric(8, 2),
  price_tax numeric(8, 2),
  discount_net numeric(8, 2),
  discount_tax numeric(8, 2),
  payment_net numeric(8, 2),
  payment_tax numeric(8, 2),
  FOREIGN KEY (product_id) REFERENCES products(id) DEFERRABLE INITIALLY IMMEDIATE,
  FOREIGN KEY (order_id) REFERENCES orders(id) DEFERRABLE INITIALLY IMMEDIATE,
  CONSTRAINT unique_order_item_and_sub_id UNIQUE (order_item_id, subqty_id)
);

CREATE TABLE IF NOT EXISTS stock (
  product_id int,
  stock int,
  purchased_stock int,
  record_timestamp timestamp,
  FOREIGN KEY (product_id) REFERENCES products(id) DEFERRABLE INITIALLY IMMEDIATE
);