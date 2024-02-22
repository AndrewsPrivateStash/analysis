/* Levenshtein appraoch */

/* brandId table */
create or replace table `rec_engine.brands_recent` as
select
  t.brandId,
  b.name,
  count(*) as txn_cnt,
  count(distinct t.userId) as user_cnt,
  sum(cast(regexp_extract(t.amount, r"\-?[0-9\.]+") as numeric)) as total_spend
from `production.transactions` t
join `production.brands` b on(b.id = t.brandId)
where parse_date("%F", date) >= "2019-01-01"
  and cast(regexp_extract(t.amount, r"\-?[0-9\.]+") as numeric) > 0
group by 1,2
order by 3 desc
;

/* mids roll */
create or replace table `rec_engine.mids_all` as
select
  m.__key__.name as mid,
  m.title as mid_name,
  r.txn_cnt,
  r.txn_amt
from `production.merchants` m
left join(
  select
    mid,
    count(distinct coalesce(orderId, externalTransactionId)) txn_cnt,
    sum(qualified_txn_amt) as txn_amt
  from `analytics_offers_views.accrued_rewards_vw`
  group by 1
) r on (r.mid = m.__key__.name)
order by txn_cnt desc, mid_name
;


create or replace table `rec_engine.brand_to_mid_map` as
with
cross_combine as(
  select *, row_number() over(partition by brand_id order by lev_scr desc) as n
  from(
    select
      b.brandId as brand_id,
      b.name as brand_name,
      m.mid,
      m.mid_name,
      functions.levdist_token_set_ratio(b.name, m.mid_name) as lev_scr
    from `rec_engine.brands_recent` b
    cross join `rec_engine.mids_all` m
  )
  where lev_scr >= 0.9
    -- remove known bad mappings
    and (brand_id, mid) not in( select (brand_id, mid) from `rec_engine.manual_badmap` )
  order by brand_id, lev_scr desc
)

select
  brand_id,
  brand_name,
  array_agg(
    struct(
      mid,
      mid_name,
      lev_scr
    )
  ) as matches

from cross_combine
where n <= 10 -- ten best matches
group by 1,2
;


create or replace table `rec_engine.mid_to_brand_map` as
with
cross_combine as(
  select *, row_number() over(partition by mid order by lev_scr desc) as n
  from(
    select
      m.mid,
      m.mid_name,
      b.brandId as brand_id,
      b.name as brand_name,
      functions.levdist_token_set_ratio(m.mid_name, b.name) as lev_scr
    from `rec_engine.mids_all` m
    cross join `rec_engine.brands_recent` b
  )
  where lev_scr >= 0.9
  order by mid, lev_scr desc
)

select
  mid,
  mid_name,
  array_agg(
    struct(
      brand_id,
      brand_name,
      lev_scr
    )
  ) as matches

from cross_combine
where n <= 10
group by 1,2
;