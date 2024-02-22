/*
  [client] sampling
    - Calc similarity across all valid users (including start time)  R-clusters
    - Split into test and control having roughly same proportion of [client] pre-history
    - Test group will get new cohort “hotel” with added campaign [client]

  brand_id: 50f8f8d9-de80-48f2-b4b6-c534adb68170
  
  method:
    - construct set of all elegible users with meta data about each (seperable features)
      > { current_day - opened lag, user-age, region, platform engagement-met }
    - cluster global set to find largest similarity group
    - seperate group into hf activity and not
    - sample x% into hold out from each of the two sub-groups
    - write results to table

*/

with

-- list of eligable users and associated cohorts
users as(
  select
    user_id,
    array_agg(cohort) as cohorts
  from(
    select
      u.id as user_id,
      c.name as cohort
    from `production.users` u
    join `production.user_cohort_links` ucl on (ucl.userId = u.id)
    join `production.cohorts` c on (c.id = ucl.cohortId)
    where c.name not in( select cohort from `analytics_views.exc_chts` )
      and u.email not like "%@bumped.com"
      and u.accountStatus = 'approved'
      and u.id not in( select userId from production.flagged_users )
  )
  group by user_id
),

-- user activation stamps
opened as(
  select
    userId as user_id,
    min(timestamp(createdAt)) acct_open
  from `bumped-analytics-aw5325.production.clearing_account_actions`
  where action = 'verifyApprovedAccount'
  group by userId
),

-- users with \[client\] activity
user_w_hf_txns as(
  select
    user_id,
    count(*) cnt
  from `analytics_views.tagged_tx_grd`
  where brand_id = '50f8f8d9-de80-48f2-b4b6-c534adb68170'
  group by 1
),

-- combine above values
user_combine as(
  select
    u.user_id,
    u.cohorts,
    o.acct_open,
    date_diff(current_date, cast(o.acct_open as date), day) +1 as acct_lag,
    h.user_id is not null as hf_act
  from users u
  join opened o on(o.user_id = u.user_id)
  left join user_w_hf_txns h on(h.user_id = u.user_id)
),

-- ### features ###
user_demo as(
  select
    id as user_id,
    date_diff(current_date, parse_date('%F', dateOfBirth), year) as age    
  from `bumped-analytics-aw5325.production.users`
  where accountStatus = 'approved'

),

geo as(
  select
    t.userId,
    za.latitude,
    za.longitude
  from (
    select *, row_number() over(partition by userId) as n
    from `bumped-analytics-aw5325.production.user_addresses`
    where type = 'HOME'
  ) t
  join `bigquery-public-data.utility_us.zipcode_area` za on(za.zipcode = t.postalCode)
  left join `bumped-analytics-aw5325.analytics_views.state_dim` sd on(sd.state_code = za.state_code)
  where n = 1 -- ensure user dim
),

user_act as(
  -- use rew_cnt velocity as platform engagement metric (no category matching HF for direct comparison)
  select
    userId,
    rew_cnt / span as rew_per_day
  from(
    select
      userId,
      date_diff(max(parse_date('%F', journalDate)), min(parse_date('%F', journalDate)), day) +1 as span,
      count(*) as rew_cnt
    from `production.rewards`
    where journalDate is not null
      and triggerType = 'transaction'
    group by 1
  )
  where span >= 14
),

combine_features as(  
  select
    u.user_id,
    u.age,
    g.latitude,
    g.longitude,
    a.rew_per_day
  from user_demo u
  join geo g on (g.userId = u.user_id)
  join user_act a on (a.userId = u.user_id)
)

-- output
select
  uc.user_id,
  uc.hf_act,
  uc.acct_lag,
  cf.* except(user_id)
from user_combine uc
join combine_features cf on (cf.user_id = uc.user_id)
;

-- R processing to cluster and sample

--summary test out
with

data as(
  select
    uf.user_id,
    uf.generation,
    uf.gender,
    uf.region,
    t.hf_act,
    round(t.rew_per_day,4) as rew_per_day,
    uf.chts,
    uf.brands 
  from `analytics_views.user_facts_vw` uf
  join `static_data.[client]_testdata` t on (t.user_id = uf.user_id)
),

cohorts as(
  select
    user_id,
    string_agg(cht, ', ') as chts
  from `analytics_views.user_facts_vw`
  cross join unnest(chts) as cht
  group by 1
),

fav_brand_and_cat as(
  select
    user_id,
    bnd.sel_brandId,
    bnd.categoryId
  from `analytics_views.user_facts_vw`
  cross join unnest(brands) as bnd
  where bnd.spd_rank = 1
)

select
  d.* except(chts, brands),
  c.chts,
  b.name as fav_brand,
  f.categoryId as fav_cat
from data d
join cohorts c on(c.user_id = d.user_id)
join fav_brand_and_cat f on (f.user_id = d.user_Id)
join `production.brands` b on(b.id = f.sel_brandId)
;

-- summary control out
with

data as(
  select
    uf.user_id,
    uf.generation,
    uf.gender,
    uf.region,
    t.hf_act,
    round(t.rew_per_day,4) as rew_per_day,
    uf.chts,
    uf.brands 
  from `analytics_views.user_facts_vw` uf
  join `static_data.[client]_ctrldata` t on (t.user_id = uf.user_id)
),

cohorts as(
  select
    user_id,
    string_agg(cht, ', ') as chts
  from `analytics_views.user_facts_vw`
  cross join unnest(chts) as cht
  group by 1
),

fav_brand_and_cat as(
  select
    user_id,
    bnd.sel_brandId,
    bnd.categoryId
  from `analytics_views.user_facts_vw`
  cross join unnest(brands) as bnd
  where bnd.spd_rank = 1
)

select
  d.* except(chts, brands),
  c.chts,
  b.name as fav_brand,
  f.categoryId as fav_cat
from data d
join cohorts c on(c.user_id = d.user_id)
join fav_brand_and_cat f on (f.user_id = d.user_Id)
join `production.brands` b on(b.id = f.sel_brandId)
;