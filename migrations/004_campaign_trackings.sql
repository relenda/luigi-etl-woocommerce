CREATE TABLE IF NOT EXISTS campaign_trackings (
  id           SERIAL,
  utm_medium   VARCHAR(255),
  utm_source   VARCHAR(255),
  utm_campaign VARCHAR(255),
  utm_term     VARCHAR(255),
  utm_content  VARCHAR(255),
  PRIMARY KEY (id)
);

ALTER TABLE orders
  ADD COLUMN last_contact_campaign_id INT,
  ADD COLUMN first_contact_campaign_id INT,
  ADD COLUMN last_contact_campaign_recorded_at TIMESTAMP,
  ADD COLUMN first_contact_campaign_recorded_at TIMESTAMP,
  ADD FOREIGN KEY (last_contact_campaign_id) REFERENCES campaign_trackings (id),
  ADD FOREIGN KEY (first_contact_campaign_id) REFERENCES campaign_trackings (id);
