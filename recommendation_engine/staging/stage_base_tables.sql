/*
collection of base tables in stage needed to execute recommendations

*/

-- ### Core transaction table tagged_tx ###
create or replace view rec_engine_staging.tagged_tx as
with names as (
  select
    row_number() over (partition by name order by count(*) desc) as row_num,
    count(*) as n, 
    brandid, 
    name
  from `bumped-analytics-aw5325.staging.transactions`
  where tagstatus = 'matched' and approved is true
  group by name, brandid
  having n > 20
  order by name, n desc
),
-- join in user via account -> links
user_tx as (
  select
    tx.transaction_id,  -- primary key in resulting view
    tx.pending_transaction_id,  -- to resolve poor id mapping to transaction table
    tx.date, 
    tx.name, 
    tx.original_description as description,
    coalesce(amount.float, amount.integer) as amount,
    account_id,
    links.userid 
  from `bumped-analytics-aw5325.staging.plaid_tx` tx
  join `bumped-analytics-aw5325.staging.plaid_accounts` accounts on (tx.account_id = accounts.id)
  join `bumped-analytics-aw5325.staging.plaid_links` links on (accounts.linkid = links.id)
  where pending = false
    -- if any of these category elements are in the category array from plaid then it is filtered
    and (select * from unnest(["credit","deposit","withdrawal","payroll","internalaccounttransfer","thirdpartyacorns","withdrawalcheck","withdrawalatm"])
      intersect distinct (select * from unnest(tx.category))) is null
),
-- join user transactions to approved names yielding a "branded" transaction
tx as (
  select
    transaction_id,
    pending_transaction_id,
    parse_date('%f', date) as tx_date,
    amount,
    account_id,
    userid as user_id,
    names.brandid as brand_id,
    user_tx.name as tx_name,
    user_tx.description as tx_description
  from user_tx
  join names using (name)
  where amount > 0 and row_num = 1
)
select 
  tx.* except (brand_id),
  b.brand_id as atomic_brand_id,
  b.parent_brand_id as brand_id,
  b.parent_brand_name as brand_name
from tx 
-- using prod view as staging.brands is not the same as production.brands
left join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = tx.brand_id)
order by tx_date desc
;

-- brands_recent
create or replace table `rec_engine_staging.brands_recent` as
select
  t.brandId,
  b.name,
  count(*) as txn_cnt,
  count(distinct t.userId) as user_cnt,
  sum(cast(regexp_extract(t.amount, r"\-?[0-9\.]+") as numeric)) as total_spend
from `staging.transactions` t
join `staging.brands` b on(b.id = t.brandId)
where parse_date("%F", date) >= "2019-01-01"
  and cast(regexp_extract(t.amount, r"\-?[0-9\.]+") as numeric) > 0
group by 1,2
order by 3 desc
;

-- accrued_rewards_vw
create or replace table `rec_engine_staging.accrued_rewards_vw` as
select
  -- stamps
  createTime,
  transactionDate,
  postDate,
  -- ids
  orderId,
  externalTransactionId,
  settlementId,
  authorizationId,
  __key__.name as accrual_id,
  trim(split(__key__.path, ", ")[offset(1)],'"') as reward_id,
  offer.name as offerId,
  userId.name as user_id,

  type,
  trim(split(offer.path, ", ")[offset(1)],'"') as mid,
  merchantDisplayName,
  -- amounts
  grossTransactionAmount.units + grossTransactionAmount.nanos / 1e9 as gross_txn_amt,
  grossTransactionAmount.currencyCode as gross_txn_amt_cur,
  qualifiedTransactionAmount.units + qualifiedTransactionAmount.nanos / 1e9 as qualified_txn_amt,
  qualifiedTransactionAmount.currencyCode as qualified_txn_amt_cur,
  rebateAmount.units + rebateAmount.nanos / 1e9 as rebate_amt,
  rebateAmount.currencyCode as rebate_amt_cur,
  
from `bumped-analytics-aw5325.staging.accrued_rewards`
;

-- mids_all
create or replace table `rec_engine_staging.mids_all` as
select
  m.__key__.name as mid,
  m.title as mid_name,
  r.txn_cnt,
  r.txn_amt
from `staging.merchants` m
left join(
  select
    mid,
    count(distinct coalesce(orderId, externalTransactionId)) txn_cnt,
    sum(qualified_txn_amt) as txn_amt
  from `rec_engine_staging.accrued_rewards_vw`
  group by 1
) r on (r.mid = m.__key__.name)
order by txn_cnt desc, mid_name
;

-- brand_to_mid_map
create or replace table `rec_engine_staging.brand_to_mid_map` as
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
    from `rec_engine_staging.brands_recent` b
    cross join `rec_engine_staging.mids_all` m
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


-- temp_[external partner]_cat_for_brands (1)
create or replace table `rec_engine_staging.temp_[external partner]_cat_for_brands` as
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
    from `rec_engine_staging.brands_recent` b
    join `rec_engine_staging.brand_to_mid_map` bmm on(bmm.brand_id = b.brandId)
    cross join bmm.matches
    where lev_scr = 1
  ) t
  join `staging.merchants` m on(m.__key__.name = t.mid and m.labels.[external partner]CategoryName is not null)
)
group by 1,2
;

-- temp_[external partner]_cat_for_brands (2)
create or replace table `rec_engine_staging.temp_[external partner]_cat_for_brands` as
with
params as(
  select
    array<string>[
      "fd90f3aa-f18d-42de-96f5-c7b9a3bdee4e",
      "pandora",
      "2a5be61c-7271-4f2b-b4e6-ab53df327b51"
    ] as rem
)

select * from `rec_engine_staging.temp_[external partner]_cat_for_brands`
where brandId not in unnest(( select rem from params ))
;

-- manual_brand_atoms
create or replace table `rec_engine_staging.manual_brand_atoms` as
select
  coalesce(c.brandId, b.brandId) as brandId,
  coalesce(c.name, b.brand_name) as brand_name,
  functions.array_union_dist(
    c.atoms,
    b.atoms
  ) as atoms
from `rec_engine_staging.temp_[external partner]_cat_for_brands` c
full outer join `rec_engine.manual_brand_atoms` b on (b.brandId = c.brandId)
;

-- mid_atoms
create or replace table `rec_engine_staging.mid_atoms` as
select
  m.__key__.name as mid,
  m.title as mid_name,
  m.labels.[external partner]CategoryName as literal_label,
  functions.atomize_attr_labels(m.labels.[external partner]CategoryName) as atoms
from `staging.merchants` m
where labels.[external partner]CategoryName is not null
;

-- user_geo_vw
create or replace view `rec_engine_staging.user_geo_vw` as
select
  struct(
    t.userId,
    t.city,
    t.postalCode as zipcode,
    za.latitude, za.longitude,
    coalesce(za.state_code, t.state) as state_code,
    za.state_name,
    za.county,
    sd.region,
    sd.division
  ) as geo

from (
  select *, row_number() over(partition by userId) as n
  from `bumped-analytics-aw5325.staging.user_addresses`
  where type = 'HOME'
) t
left join `bigquery-public-data.utility_us.zipcode_area` za on(za.zipcode = t.postalCode)
left join `bumped-analytics-aw5325.analytics_views.state_dim` sd on(sd.state_code = coalesce(za.state_code, t.state))
where n = 1
;

-- active_offers_vw
create or replace view `rec_engine_staging.active_offers_vw` as(
  select * from(
  select
    *,
    row_number() over(partition by offer_id, mid order by revision_number desc) as n
  from(
    select
      split(name, '/')[offset(1)] as mid,
      split(name, '/')[offset(3)] as offer_id,
      title,
      merchant_title,
      aggregator,
      aggregator_offer_id,
      url,
      revision_number
    from `offers_staging.publication_revisions`
    where
      unpublish_time is null
      and status = "ACTIVE"
      and date(end_time) > current_date
  ))
  where n = 1
);


-- static_daily_recs
create or replace table `rec_engine_staging.static_daily_recs` as
with
users as(
  select id as user_id from staging.users
)

select * from unnest( rec_engine_staging.rec_n( array(select user_id from users), current_date, null) )
;
