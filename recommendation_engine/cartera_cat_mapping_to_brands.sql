-- use [external partner] categories to populate [provider] brands
create or replace table `rec_engine.temp_[external partner]_cat_for_brands` as
select
  brandId,
  name,
  string_agg(distinct mid_name) as mids,
  functions.atomize_attr_labels(
    string_agg(category, " ")
  ) as atoms

from(
  select
    brandId,
    name,
    mid,
    mid_name,
    m.labels.[external partner]CategoryName as category
  from(
    select *
    from `rec_engine.brands_recent` b
    join `rec_engine.brand_to_mid_map` bmm on(bmm.brand_id = b.brandId)
    cross join bmm.matches
    where lev_scr = 1
  ) t
  join `production.merchants` m on(m.__key__.name = t.mid and m.labels.[external partner]CategoryName is not null)
)
group by 1,2
;

-- remove bad matches (recursive)
create or replace table `rec_engine.temp_[external partner]_cat_for_brands` as
with
params as(
  select
    array<string>[
      "fd90f3aa-f18d-42de-96f5-c7b9a3bdee4e",
      "pandora",
      "2a5be61c-7271-4f2b-b4e6-ab53df327b51"
    ] as rem
)

select * from `rec_engine.temp_[external partner]_cat_for_brands`
where brandId not in unnest(( select rem from params ))
;