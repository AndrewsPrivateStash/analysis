/*
    manual atoms for top 100 brands from [provider]
    https://docs.google.com/spreadsheets/d/19VuzpBTrY3w3zAxmpRv8ESuyIcI3gmHv8Q8uLNAeigo/edit?usp=sharing

    combined with extracted atoms from [external partner] matched brands found in:
    table: `rec_engine.temp_[external partner]_cat_for_brands`
    perso_engine/[external partner]_cat_mapping_to_brands.sql

*/


create or replace table `rec_engine.manual_brand_atoms` as
select
  coalesce(c.brandId, b.brandId) as brandId,
  coalesce(c.name, b.brand_name) as brand_name,
  functions.array_union_dist(
    c.atoms,
    split(b.atoms, ",")
  ) as atoms
from `rec_engine.temp_[external partner]_cat_for_brands` c
full outer join `rec_engine.manual_brand_atoms` b on (b.brandId = c.brandId)
;