create or replace table `rec_engine.mid_atoms` as
select
  m.__key__.name as mid,
  m.title as mid_name,
  m.labels.carteraCategoryName as literal_label,
  functions.atomize_attr_labels(m.labels.carteraCategoryName) as atoms
from `production.merchants` m
where labels.carteraCategoryName is not null
;