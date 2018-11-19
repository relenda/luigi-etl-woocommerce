CREATE TABLE IF NOT EXISTS category_tree (
  term_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name0 varchar(255),
  name1 varchar(255),
  name2 varchar(255),
  name3 varchar(255)
);

TRUNCATE category_tree;

INSERT INTO category_tree (term_id, name0, name1, name2, name3)

SELECT
	COALESCE(tax3.term_id, tax2.term_id, tax1.term_id, tax0.term_id) as term_id,
	term0.name as name0,

	REPLACE(term1.name, 'amp;', '') as name1,
	REPLACE(term2.name, 'amp;', '') as name2,
	REPLACE(term3.name, 'amp;', '') as name3

FROM `wpshop_term_taxonomy` tax0
INNER JOIN `wpshop_terms` term0 ON tax0.term_id = term0.term_id

LEFT JOIN `wpshop_term_taxonomy` tax1 ON tax0.term_id = tax1.parent
LEFT JOIN `wpshop_terms` term1 ON tax1.term_id = term1.term_id

LEFT JOIN `wpshop_term_taxonomy` tax2 ON tax1.term_id = tax2.parent
LEFT JOIN `wpshop_terms` term2 ON tax2.term_id = term2.term_id

LEFT JOIN `wpshop_term_taxonomy` tax3 ON tax2.term_id = tax3.parent
LEFT JOIN `wpshop_terms` term3 ON tax3.term_id = term3.term_id

WHERE tax0.taxonomy = 'product_cat'
AND tax0.parent = 0

UNION

-- add all categories which had children in the previous step for each depth

-- depth 2

SELECT
	tax2.term_id as id,
	term0.name as name0,

	REPLACE(term1.name, 'amp;', '') as name1,
	REPLACE(term2.name, 'amp;', '') as name2,
	NULL

FROM `wpshop_term_taxonomy` tax0
INNER JOIN `wpshop_terms` term0 ON tax0.term_id = term0.term_id

LEFT JOIN `wpshop_term_taxonomy` tax1 ON tax0.term_id = tax1.parent
LEFT JOIN `wpshop_terms` term1 ON tax1.term_id = term1.term_id

LEFT JOIN `wpshop_term_taxonomy` tax2 ON tax1.term_id = tax2.parent
LEFT JOIN `wpshop_terms` term2 ON tax2.term_id = term2.term_id

LEFT JOIN `wpshop_term_taxonomy` tax3 ON tax2.term_id = tax3.parent

WHERE tax0.taxonomy = 'product_cat'
AND tax0.parent = 0
AND tax3.term_id IS NOT NULL
GROUP BY tax2.term_id, term0.name, term1.name, term2.name

UNION

-- depth 1

SELECT
	tax1.term_id as id,
	term0.name as name0,

	REPLACE(term1.name, 'amp;', '') as name1,
	NULL,
	NULL

FROM `wpshop_term_taxonomy` tax0
INNER JOIN `wpshop_terms` term0 ON tax0.term_id = term0.term_id

LEFT JOIN `wpshop_term_taxonomy` tax1 ON tax0.term_id = tax1.parent
LEFT JOIN `wpshop_terms` term1 ON tax1.term_id = term1.term_id

LEFT JOIN `wpshop_term_taxonomy` tax2 ON tax1.term_id = tax2.parent

WHERE tax0.taxonomy = 'product_cat'
AND tax0.parent = 0
AND tax2.term_id IS NOT NULL
GROUP BY tax1.term_id, term1.name, term0.name

UNION

-- depth 0

SELECT
	tax0.term_id as id,
	term0.name as name0,
	NULL,
	NULL,
	NULL
FROM `wpshop_term_taxonomy` tax0
INNER JOIN `wpshop_terms` term0 ON tax0.term_id = term0.term_id

INNER JOIN `wpshop_term_taxonomy` tax1 ON tax0.term_id = tax1.parent

WHERE tax0.taxonomy = 'product_cat'
AND tax0.parent = 0
AND tax1.term_id IS NOT NULL
GROUP BY tax0.term_id

ORDER BY name0, name1, name2, name3;
