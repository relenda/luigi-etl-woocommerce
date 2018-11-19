SELECT
  variation.ID         AS product_id,
  parent.post_title    AS name,
  parent.ID            AS parent_id,
  (
    SELECT meta_value
    FROM wpshop_postmeta m
    WHERE m.post_id = parent.ID AND
          m.meta_key = '_thumbnail_id'
  )                    AS thumbnail_id,
  (
    -- FIXME: old datasets have still babysachen-mieten as URL
    SELECT REPLACE(p.guid, 'shop.babysachen-mieten.de', 'kilenda.de')
    FROM wpshop_postmeta m
      INNER JOIN wpshop_posts p ON m.meta_value = p.ID
    WHERE m.post_id = parent.ID AND
          m.meta_key = '_thumbnail_id'
  )                    AS thumbnail_url,
  (
    SELECT term.name
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'product_cat'
    LIMIT 1
  )                    AS category,
  (
    SELECT category.name1
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
      INNER JOIN category_tree category USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'product_cat'
    LIMIT 1
  )                    AS primary_category,
  (
    SELECT category.name0
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
      INNER JOIN category_tree category USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'product_cat'
    LIMIT 1
  )                    AS super_category,

  -- pricing, profit margin and taxes
  (
    #FIXME
    SELECT meta_value
    FROM wpshop_postmeta m
    WHERE m.post_id = parent.ID AND
          m.meta_key = '_uvp'
  )                    AS uvp,
  (
    SELECT meta_value
    FROM wpshop_postmeta m
    WHERE m.post_id = parent.ID AND
          m.meta_key = '_cogs'
  )                    AS cost_of_goods,

  -- product attributes
  (
    SELECT GROUP_CONCAT(term.name SEPARATOR ', ')
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_farbe'
  )                    AS color,
  (
    SELECT COALESCE(GROUP_CONCAT(term.name SEPARATOR ', '), 'Altbestand')
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_marke'
  )                    AS brand,
  (
    SELECT GROUP_CONCAT(term.name SEPARATOR ', ')
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_order_saison'
  )                    AS order_season,

  -- season features
  (
    SELECT 1
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_jahreszeit'
          AND term.slug = 'fruehling'
    UNION
    SELECT 0
    LIMIT 1
  )                    AS season_fruehling,
  (
    SELECT 1
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_jahreszeit'
          AND term.slug = 'sommer'
    UNION
    SELECT 0
    LIMIT 1
  )                    AS season_sommer,
  (
    SELECT 1
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_jahreszeit'
          AND term.slug = 'herbst'
    UNION
    SELECT 0
    LIMIT 1
  )                    AS season_herbst,
  (
    SELECT 1
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_jahreszeit'
          AND term.slug = 'winter'
    UNION
    SELECT 0
    LIMIT 1
  )                    AS season_winter,

  -- sex
  (
    SELECT IF(COUNT(*) > 1, 'beide', GROUP_CONCAT(term.slug))
    FROM wpshop_term_relationships rel
      INNER JOIN wpshop_term_taxonomy tax USING (term_taxonomy_id)
      INNER JOIN wpshop_terms term USING (term_id)
    WHERE 1
          AND object_id = parent.ID
          AND tax.taxonomy = 'pa_geschlecht'
    GROUP BY object_id
  )                    AS sex,

  -- variation features
  (
    SELECT term.name
    FROM wpshop_postmeta m
      INNER JOIN wpshop_terms term ON term.slug = m.meta_value
    WHERE m.post_id = variation.ID AND
          m.meta_key IN (
            'attribute_pa_groesse',
            'attribute_pa_um_groesse',
            'attribute_pa_sgroessen',
            'attribute_pa_handschuhgroesse',
            'attribute_pa_schalgroesse',
            'attribute_pa_kopfumfang',
            'attribute_pa_abenteuergroesse',
            'attribute_pa_kgr'
          )
    LIMIT 1
  )                    AS size,
  (
    -- FIXME
    SELECT term.name
    FROM wpshop_postmeta m
      INNER JOIN wpshop_terms term ON term.slug = m.meta_value
    WHERE m.post_id = variation.ID AND
          m.meta_key = 'attribute_pa_groesse'
    LIMIT 1
  )                    AS size_shopfilter,
  (
    SELECT meta_value
    FROM wpshop_postmeta m
    WHERE m.post_id = variation.ID AND
          m.meta_key = 'attribute_pa_zustand'
  )                    AS item_condition,
  last_order_at        AS last_order_at,
  -- version information
  parent.post_date     AS created_at,
  parent.post_modified AS updated_at
FROM wpshop_posts variation
  INNER JOIN wpshop_posts parent ON variation.post_parent = parent.ID
  -- this is for performance reasons a join
  LEFT JOIN products__last_order_date
    ON products__last_order_date.product_id = variation.ID
WHERE 1
      AND variation.post_type = 'product_variation'
      AND variation.ID >= 7294 -- exclude corrupted old records
      AND parent.post_status = 'publish'
;