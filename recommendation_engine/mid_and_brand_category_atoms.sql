/*
  grab all mid attributes given by [external partner]
*/

create or replace view `rec_engine.mid_category_label_atoms` as
select
  label,
  any_value(literal_label) as literal_sample,
  count(*) as cnt

from(
  select
    title,
    labels.[external partner]CategoryName as literal_label,
    functions.atomize_attr_labels(labels.[external partner]CategoryName) as [external partner]_labels
  from `production.merchants`
  where labels.[external partner]CategoryName is not null
)
cross join unnest([external partner]_labels) as label
group by 1
order by 3 desc
;


/*
  grab all category labels associated with all known brands
*/
create or replace view `rec_engine.brand_category_label_atoms` as
with
brand_tbl as(
  select atom, any_value(name) as sample_label, count(*) as cnt
  from(
    select functions.atomize_attr_labels(c.name) as atoms, c.name
    from `production.brands` b
    join `production.brand_categories` c on (c.id = b.categoryId)
  ) t
  cross join unnest(atoms) as atom
  group by 1
),

loy_tbl as(
  select atom, any_value(name) as sample_label, count(*) as cnt
  from(
    select functions.atomize_attr_labels(c.name) as atoms, c.name
    from `production.user_loyalty` l
    join `production.brand_categories` c on (c.id = l.categoryId)
  ) t
  cross join unnest(atoms) as atom
  group by 1
)

select
  coalesce(b.atom, l.atom) as label,
  functions.lat_rand([b.sample_label, l.sample_label]) as literal_sample,
  coalesce(b.cnt,0) + coalesce(l.cnt,0) as cnt
from brand_tbl b
full outer join loy_tbl l on(l.atom = b.atom)
order by 3 desc, 1
;

-- combine the two above
create or replace view `rec_engine.all_category_label_atoms` as
select
  coalesce(b.label, m.label) as atom,
  array_to_string([b.literal_sample, m.literal_sample], ',') as literal_sample,
  coalesce(b.cnt,0) + coalesce(m.cnt,0) as cnt
from `rec_engine.brand_category_label_atoms` b
full outer join `rec_engine.mid_category_label_atoms` m on(lower(m.label) = lower(b.label))
;


-- missing MIDs
select m.*
from `rec_engine.mids_all` m
join `production.merchants` pm on(pm.__key__.name = m.mid )
where pm.labels.[external partner]CategoryName is null