/*
  ROI query for given brands before a given date, with a given window
  f( brandId, uprBnd, win ) -> { preObs, postObs, preVis, postVis, preSpend, postSpend, postRew, delVis, delSpend, ROI }
  
  -- COVID cut date: 2020-02-25
  -- offer start date: 2020-09-29
  -- end of legacy rewards date: 2020-10-31

*/

with
params as(
  select
    array<string>["lowes", "the-home-depot"] as brand_ids,
    date "2020-02-25" as upr_bnd,
    90 as win,
    date "2020-10-31" as legacy_end,
    functions.exc_cht_names([]) as exc_cht
),

valid_users as(
  select *
  from `analytics_offers_views.user_segments_vw`
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus = "approved"
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), least((select legacy_end from params),(select upr_bnd from params)) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
      and l.brandId in unnest(( select brand_ids from params ))
  ))
  where dur_sort = 1
),

users as(
  select
    v.user_id,
    c.brand_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join analytics_views.brand_campaign_rates_vw c on(c.cohort_name = cht)
  group by 1, 2
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    t.brand_id in unnest(( select brand_ids from params ))
    -- transaction windowing
    and date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))    -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      brand_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.brand_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id and u.brand_id = r.brand_id) -- filter users and bring in reward percent
    group by 1,2
  )
)

-- output metrics
select
  b.name as brand_name,
  countif(w.pre_tot_txn is not null) as pre_users,
  countif(w.post_tot_txn is not null) as post_users,
  countif(w.del_mo_amt > 0) as chg_user_cnt,
  countif(w.del_mo_amt > 0) / countif(w.post_tot_txn is not null) as pct_chg_users,
  
  -- simple avgerages
  avg(w.pre_avg_txn) as avg_pre_txns_mo,
  avg(w.pre_avg_amt) as avg_pre_amt_mo,
  avg(w.post_avg_txn) as avg_post_txns_mo,
  avg(w.post_avg_amt) as avg_post_amt_mo,
  avg(w.del_mo_txn) as avg_del_txns_mo,
  avg(w.del_mo_amt) as avg_del_amt_mo,
  avg(w.del_mo_txn) / avg(w.pre_avg_txn) as avg_txn_chg_mo,
  avg(w.del_mo_amt) / avg(w.pre_avg_amt) as avg_amt_chg_mo,

  -- global top-down values
  sum(w.del_tot_txn) / sum(w.pre_tot_txn) as top_txn_chg,
  sum(w.del_tot_amt) / sum(w.pre_tot_amt) as top_amt_chg,
  sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
  (sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) / sum(w.rew_amt) as roi

from wide_user w
join `production.brands` b on(b.id = w.brand_id)
where
  -- arbitrary assumptions
  --pre_tot_txn is not null
  --and post_tot_txn is not null
  true
group by 1
;


-- roll all brands
with
params as(
  select
    date "2020-10-31" as upr_bnd,
    90 as win,
    date "2020-10-31" as legacy_end,
    functions.exc_cht_names([]) as exc_cht
),

valid_users as(
  select *
  from `analytics_offers_views.user_segments_vw`
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus = "approved"
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), least((select legacy_end from params),(select upr_bnd from params)) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
  ))
  where dur_sort = 1
),

users as(
  select
    v.user_id,
    c.brand_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join analytics_views.brand_campaign_rates_vw c on(c.cohort_name = cht)
  group by 1, 2
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    -- transaction windowing
    date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))        -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      brand_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.brand_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id and u.brand_id = r.brand_id) -- filter users and bring in reward percent
    group by 1,2
  )
),

data as(
  select
    t.*,
    t.avg_del_amt_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as lwr_05_diff_spend,
    t.avg_del_amt_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as upr_05_diff_spend
  
  from(
  select
    b.name as brand_name,
    count(*) as obs,
    countif(w.pre_tot_txn is not null) as pre_users,
    countif(w.post_tot_txn is not null) as post_users,
    countif(w.del_mo_amt > 0) as chg_user_cnt,
    safe_divide(countif(w.del_mo_amt > 0) , countif(w.post_tot_txn is not null)) as pct_chg_users,

    --ticket values
    avg(w.pre_avg_amt / w.pre_avg_txn) as avg_pre_ticket,
    avg(w.post_avg_amt / w.post_avg_txn) as avg_post_ticket,
    -- simple avgerages
    avg(w.pre_avg_txn) as avg_pre_txns_mo,
    avg(w.pre_avg_amt) as avg_pre_amt_mo,
    avg(w.post_avg_txn) as avg_post_txns_mo,
    avg(w.post_avg_amt) as avg_post_amt_mo,
    avg(w.del_mo_txn) as avg_del_txns_mo,
    avg(w.del_mo_amt) as avg_del_amt_mo,
    safe_divide(avg(w.del_mo_txn) , avg(w.pre_avg_txn)) as avg_txn_chg_mo,
    safe_divide(avg(w.del_mo_amt) , avg(w.pre_avg_amt)) as avg_amt_chg_mo,

    -- global top-down values
    safe_divide(sum(w.del_tot_txn) , sum(w.pre_tot_txn)) as top_txn_chg,
    safe_divide(sum(w.del_tot_amt) , sum(w.pre_tot_amt)) as top_amt_chg,
    sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
    safe_divide((sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) , sum(w.rew_amt)) as roi,

    -- stats
    stddev_samp(w.del_mo_amt) as std_del_mo_amt,
    stddev_samp(w.del_mo_amt) / sqrt(count(*)) as se_del_mo_amt

  from wide_user w
  join `production.brands` b on(b.id = w.brand_id)
  where w.post_tot_txn is not null -- make sure users are all shareholders
  group by 1
  ) as t
  left join `bumped-analytics-aw5325.analytics_views.t_stats` s on (t.obs -1 = s.df)
  where
    pre_users >= 50 and post_users >= 50
)

-- output
select
  brand_name,
  obs, pre_users, post_users, pct_chg_users,
  avg_pre_txns_mo, avg_post_txns_mo, avg_del_txns_mo,
  avg_pre_amt_mo, avg_post_amt_mo, avg_del_amt_mo,
  
  avg_pre_ticket, avg_post_ticket, avg_post_ticket - avg_pre_ticket as ticket_diff,
  safe_divide(avg_post_ticket - avg_pre_ticket, avg_pre_ticket) as ticket_pct_chg,
  
  avg_txn_chg_mo,
  avg_amt_chg_mo,
  roi,
  lwr_05_diff_spend,
  upr_05_diff_spend,
  se_del_mo_amt,
  spend_sig
from(
select
  *,
  case
    when lwr_05_diff_spend > 0 then true
    when upr_05_diff_spend < 0 then true
    else false
  end as spend_sig
from data
)
order by brand_name
;


-- Nike request from David
with
params as(
  select
    array<string>["nike"] as brand_ids,
    date "2020-02-25" as upr_bnd,
    90 as win,
    date "2020-10-31" as legacy_end,
    functions.exc_cht_names(["e","o"]) as exc_cht
),

valid_users as(
  select *
  from `analytics_offers_views.user_segments_vw`
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus = "approved"
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), least((select legacy_end from params),(select upr_bnd from params)) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
      and l.brandId in unnest(( select brand_ids from params ))
  ))
  where dur_sort = 1
),

camp as (
-- grab reward rate for brand/cohort
-- chosing most recent rew_rate forcing dim: brand/cohort
  select * except(n)
  from(
    select
      campaigns.brandId as brand_id,
      c.name as cohort_name,
      parse_date('%F', campaigns.startDate) as start_date,
      parse_date('%F', campaigns.endDate) as end_date,
      cast(regexp_extract(campaigns.rewardPercentage, r"([\d\.]+)") as numeric) * 0.01 as rewardPercentage,
      row_number() over (
                    partition by campaigns.brandId, c.name
                    order by
                      coalesce(parse_date('%F', campaigns.endDate), date '2999-01-01') desc,          -- null wins, otherwise max enddate
                      cast(regexp_extract(campaigns.rewardPercentage, r"([\d\.]+)") as numeric) desc  -- control for dup, choose max rate if conflict
                   ) n
    from `bumped-analytics-aw5325.production.cohort_campaign_links` as links
    join `bumped-analytics-aw5325.production.campaigns` as campaigns on (links.campaignId = campaigns.id)
    join `production.cohorts` c on (c.id = links.cohortId)
    where campaigns.brandId in unnest(( select brand_ids from params ))
  )
  where n = 1 -- remove overlaping/dup reward rows
),

users as(
  select
    v.user_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join camp c on(c.cohort_name = cht)
  group by 1
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    t.brand_id in unnest(( select brand_ids from params ))
    -- transaction windowing
    and date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))    -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      brand_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.brand_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id) -- filter users and bring in reward percent
    group by 1,2
  )
)

-- output metrics
select
  b.name as brand_name,
  countif(w.pre_tot_txn is not null) as pre_users,
  countif(w.post_tot_txn is not null) as post_users,
  countif(w.del_mo_amt > 0) as chg_user_cnt,
  countif(w.del_mo_amt > 0) / countif(w.post_tot_txn is not null) as pct_chg_users,
  
  -- simple avgerages
  avg(w.pre_avg_txn) as avg_pre_txns_mo,
  avg(w.pre_avg_amt) as avg_pre_amt_mo,
  avg(w.post_avg_txn) as avg_post_txns_mo,
  avg(w.post_avg_amt) as avg_post_amt_mo,
  avg(w.del_mo_txn) as avg_del_txns_mo,
  avg(w.del_mo_amt) as avg_del_amt_mo,
  avg(w.del_mo_txn) / avg(w.pre_avg_txn) as avg_txn_chg_mo,
  avg(w.del_mo_amt) / avg(w.pre_avg_amt) as avg_amt_chg_mo,

  -- global top-down values
  sum(w.del_tot_txn) / sum(w.pre_tot_txn) as top_txn_chg,
  sum(w.del_tot_amt) / sum(w.pre_tot_amt) as top_amt_chg,
  sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
  (sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) / sum(w.rew_amt) as roi

from wide_user w
join `production.brands` b on(b.id = w.brand_id)
where
  -- arbitrary assumptions
  --pre_tot_txn is not null
  post_tot_txn is not null
group by 1
;




-- roll all brands with currentdate upper bound
create or replace table `marketing_analytics.all_brand_data` as
with
params as(
  select
    current_date as upr_bnd,
    90 as win,
    functions.exc_cht_names(['e']) as exc_cht
),

valid_users as(
  select *
  from analytics_views.all_user_segments
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus in ("approved", "deactivated")
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), (select upr_bnd from params) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
  ))
  where dur_sort = 1
),

users as(
  select
    v.user_id,
    c.brand_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join analytics_views.brand_campaign_rates_vw c on(c.cohort_name = cht)
  group by 1, 2
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    -- transaction windowing
    date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))    -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      brand_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.brand_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id and u.brand_id = r.brand_id) -- filter users and bring in reward percent
    group by 1,2
  )
),

data as(
  select
    t.*,
    t.avg_del_txns_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_txn as lwr_05_diff_txn,
    t.avg_del_txns_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_txn as upr_05_diff_txn,
    t.avg_del_amt_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as lwr_05_diff_spend,
    t.avg_del_amt_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as upr_05_diff_spend
  
  from(
  select
    b.name as brand_name,
    count(*) as obs,
    countif(w.pre_tot_txn is not null) as pre_users,
    countif(w.post_tot_txn is not null) as post_users,
    countif(w.del_mo_amt > 0) as chg_user_cnt,
    safe_divide(countif(w.del_mo_amt > 0) , countif(w.post_tot_txn is not null)) as pct_chg_users,
    
    -- raw values
    sum(w.pre_tot_txn) as pre_tot_txns,
    sum(w.post_tot_txn) as post_tot_txns,
    sum(w.pre_tot_amt) as pre_tot_amt,
    sum(w.post_tot_amt) as post_tot_amt,

    --ticket values
    avg(w.pre_avg_amt / w.pre_avg_txn) as avg_pre_ticket,
    avg(w.post_avg_amt / w.post_avg_txn) as avg_post_ticket,
    -- simple avgerages
    avg(w.pre_avg_txn) as avg_pre_txns_mo,
    avg(w.pre_avg_amt) as avg_pre_amt_mo,
    avg(w.post_avg_txn) as avg_post_txns_mo,
    avg(w.post_avg_amt) as avg_post_amt_mo,
    avg(w.del_mo_txn) as avg_del_txns_mo,
    avg(w.del_mo_amt) as avg_del_amt_mo,
    safe_divide(avg(w.del_mo_txn) , avg(w.pre_avg_txn)) as avg_txn_chg_mo,
    safe_divide(avg(w.del_mo_amt) , avg(w.pre_avg_amt)) as avg_amt_chg_mo,

    -- global top-down values
    safe_divide(sum(w.del_tot_txn) , sum(w.pre_tot_txn)) as top_txn_chg,
    safe_divide(sum(w.del_tot_amt) , sum(w.pre_tot_amt)) as top_amt_chg,
    sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
    safe_divide((sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) , sum(w.rew_amt)) as roi,

    -- stats
    stddev_samp(w.del_mo_txn) as std_del_mo_txn,
    stddev_samp(w.del_mo_txn) / sqrt(count(*)) as se_del_mo_txn,
    stddev_samp(w.del_mo_amt) as std_del_mo_amt,
    stddev_samp(w.del_mo_amt) / sqrt(count(*)) as se_del_mo_amt,

  from wide_user w
  join `production.brands` b on(b.id = w.brand_id)
  where w.post_tot_txn is not null -- make sure users are all shareholders
  group by 1
  ) as t
  left join `bumped-analytics-aw5325.analytics_views.t_stats` s on (t.obs -1 = s.df)
  where
    pre_users >= 20 and post_users >= 20
)

-- output
select
  brand_name,
  obs, pre_users, post_users, pct_chg_users,
  pre_tot_txns, post_tot_txns,
  pre_tot_amt, post_tot_amt,
  
  avg_pre_txns_mo, avg_post_txns_mo, avg_del_txns_mo,
  avg_pre_amt_mo, avg_post_amt_mo, avg_del_amt_mo,
  
  avg_pre_ticket, avg_post_ticket, avg_post_ticket - avg_pre_ticket as ticket_diff,
  safe_divide(avg_post_ticket - avg_pre_ticket, avg_pre_ticket) as ticket_pct_chg,
  
  avg_txn_chg_mo,
  avg_amt_chg_mo,
  roi,
  lwr_05_diff_spend,
  upr_05_diff_spend,
  se_del_mo_amt,
  spend_sig,
  visit_sig
from(
select
  *,
  case
    when lwr_05_diff_txn > 0 then true
    when upr_05_diff_txn < 0 then true
    else false
  end as visit_sig,
  case
    when lwr_05_diff_spend > 0 then true
    when upr_05_diff_spend < 0 then true
    else false
  end as spend_sig
from data
)
order by 1
;

-- updated pre-COVID data
with
params as(
  select
    date "2020-02-25" as upr_bnd,
    90 as win,
    date "2020-10-31" as legacy_end,
    functions.exc_cht_names(['e']) as exc_cht
),

valid_users as(
  select *
  from `analytics_offers_views.user_segments_vw`
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus = "approved"
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), least((select legacy_end from params),(select upr_bnd from params)) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
  ))
  where dur_sort = 1
),

users as(
  select
    v.user_id,
    c.brand_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join analytics_views.brand_campaign_rates_vw c on(c.cohort_name = cht)
  group by 1, 2
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    -- transaction windowing
    date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))    -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      brand_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.brand_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id and u.brand_id = r.brand_id) -- filter users and bring in reward percent
    group by 1,2
  )
),

data as(
  select
    t.*,
    t.avg_del_amt_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as lwr_05_diff_spend,
    t.avg_del_amt_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as upr_05_diff_spend
  
  from(
  select
    b.name as brand_name,
    count(*) as obs,
    countif(w.pre_tot_txn is not null) as pre_users,
    countif(w.post_tot_txn is not null) as post_users,
    countif(w.del_mo_amt > 0) as chg_user_cnt,
    safe_divide(countif(w.del_mo_amt > 0) , countif(w.post_tot_txn is not null)) as pct_chg_users,

    --ticket values
    avg(w.pre_avg_amt / w.pre_avg_txn) as avg_pre_ticket,
    avg(w.post_avg_amt / w.post_avg_txn) as avg_post_ticket,
    -- simple avgerages
    avg(w.pre_avg_txn) as avg_pre_txns_mo,
    avg(w.pre_avg_amt) as avg_pre_amt_mo,
    avg(w.post_avg_txn) as avg_post_txns_mo,
    avg(w.post_avg_amt) as avg_post_amt_mo,
    avg(w.del_mo_txn) as avg_del_txns_mo,
    avg(w.del_mo_amt) as avg_del_amt_mo,
    safe_divide(avg(w.del_mo_txn) , avg(w.pre_avg_txn)) as avg_txn_chg_mo,
    safe_divide(avg(w.del_mo_amt) , avg(w.pre_avg_amt)) as avg_amt_chg_mo,

    -- global top-down values
    safe_divide(sum(w.del_tot_txn) , sum(w.pre_tot_txn)) as top_txn_chg,
    safe_divide(sum(w.del_tot_amt) , sum(w.pre_tot_amt)) as top_amt_chg,
    sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
    safe_divide((sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) , sum(w.rew_amt)) as roi,

    -- stats
    stddev_samp(w.del_mo_amt) as std_del_mo_amt,
    stddev_samp(w.del_mo_amt) / sqrt(count(*)) as se_del_mo_amt

  from wide_user w
  join `production.brands` b on(b.id = w.brand_id)
  where w.post_tot_txn is not null -- make sure users are all shareholders
  group by 1
  ) as t
  left join `bumped-analytics-aw5325.analytics_views.t_stats` s on (t.obs -1 = s.df)
  where
    pre_users >= 20 and post_users >= 20
)

-- output
select
  brand_name,
  obs, pre_users, post_users, pct_chg_users,
  avg_pre_txns_mo, avg_post_txns_mo, avg_del_txns_mo,
  avg_pre_amt_mo, avg_post_amt_mo, avg_del_amt_mo,
  
  avg_pre_ticket, avg_post_ticket, avg_post_ticket - avg_pre_ticket as ticket_diff,
  safe_divide(avg_post_ticket - avg_pre_ticket, avg_pre_ticket) as ticket_pct_chg,
  
  avg_txn_chg_mo,
  avg_amt_chg_mo,
  roi,
  lwr_05_diff_spend,
  upr_05_diff_spend,
  se_del_mo_amt,
  spend_sig
from(
select
  *,
  case
    when lwr_05_diff_spend > 0 then true
    when upr_05_diff_spend < 0 then true
    else false
  end as spend_sig
from data
)
order by brand_name
;




-- roll all categories with currentdate upper bound
create or replace table `marketing_analytics.all_cat_data` as
with
params as(
  select
    current_date as upr_bnd,
    90 as win,
    functions.exc_cht_names(['e']) as exc_cht
),

valid_users as(
  select *
  from analytics_views.all_user_segments
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus in ("approved", "deactivated")
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), (select upr_bnd from params) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
  ))
  where dur_sort = 1
),

users as(
  select
    v.user_id,
    b.category_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join analytics_views.brand_campaign_rates_vw c on(c.cohort_name = cht)
  join `analytics_views.brands_vw_2` b on(b.brand_id = c.brand_id)
  group by 1, 2
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    b.category_id,
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    -- transaction windowing
    date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))    -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      category_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.category_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id and u.category_id = r.category_id) -- filter users and bring in reward percent
    group by 1,2
  )
),

data as(
  select
    t.*,
    t.avg_del_txns_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_txn as lwr_05_diff_txn,
    t.avg_del_txns_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_txn as upr_05_diff_txn,
    t.avg_del_amt_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as lwr_05_diff_spend,
    t.avg_del_amt_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as upr_05_diff_spend
  
  from(
  select
    b.name as category_name,
    count(*) as obs,
    countif(w.pre_tot_txn is not null) as pre_users,
    countif(w.post_tot_txn is not null) as post_users,
    countif(w.del_mo_amt > 0) as chg_user_cnt,
    safe_divide(countif(w.del_mo_amt > 0) , countif(w.post_tot_txn is not null)) as pct_chg_users,
    
    -- raw values
    sum(w.pre_tot_txn) as pre_tot_txns,
    sum(w.post_tot_txn) as post_tot_txns,
    sum(w.pre_tot_amt) as pre_tot_amt,
    sum(w.post_tot_amt) as post_tot_amt,

    --ticket values
    avg(w.pre_avg_amt / w.pre_avg_txn) as avg_pre_ticket,
    avg(w.post_avg_amt / w.post_avg_txn) as avg_post_ticket,
    -- simple avgerages
    avg(w.pre_avg_txn) as avg_pre_txns_mo,
    avg(w.pre_avg_amt) as avg_pre_amt_mo,
    avg(w.post_avg_txn) as avg_post_txns_mo,
    avg(w.post_avg_amt) as avg_post_amt_mo,
    avg(w.del_mo_txn) as avg_del_txns_mo,
    avg(w.del_mo_amt) as avg_del_amt_mo,
    safe_divide(avg(w.del_mo_txn) , avg(w.pre_avg_txn)) as avg_txn_chg_mo,
    safe_divide(avg(w.del_mo_amt) , avg(w.pre_avg_amt)) as avg_amt_chg_mo,

    -- global top-down values
    safe_divide(sum(w.del_tot_txn) , sum(w.pre_tot_txn)) as top_txn_chg,
    safe_divide(sum(w.del_tot_amt) , sum(w.pre_tot_amt)) as top_amt_chg,
    sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
    safe_divide((sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) , sum(w.rew_amt)) as roi,

    -- stats
    stddev_samp(w.del_mo_txn) as std_del_mo_txn,
    stddev_samp(w.del_mo_txn) / sqrt(count(*)) as se_del_mo_txn,
    stddev_samp(w.del_mo_amt) as std_del_mo_amt,
    stddev_samp(w.del_mo_amt) / sqrt(count(*)) as se_del_mo_amt,
    

  from wide_user w
  join `production.brand_categories` b on(b.id = w.category_id)
  where w.post_tot_txn is not null -- make sure users are all shareholders
  group by 1
  ) as t
  left join `bumped-analytics-aw5325.analytics_views.t_stats` s on (t.obs -1 = s.df)
  where
    pre_users >= 20 and post_users >= 20
)

-- output
select
  category_name,
  obs, pre_users, post_users, pct_chg_users,
  pre_tot_txns, post_tot_txns,
  pre_tot_amt, post_tot_amt,
  
  avg_pre_txns_mo, avg_post_txns_mo, avg_del_txns_mo,
  avg_pre_amt_mo, avg_post_amt_mo, avg_del_amt_mo,
  
  avg_pre_ticket, avg_post_ticket, avg_post_ticket - avg_pre_ticket as ticket_diff,
  safe_divide(avg_post_ticket - avg_pre_ticket, avg_pre_ticket) as ticket_pct_chg,
  
  avg_txn_chg_mo,
  avg_amt_chg_mo,
  roi,
  lwr_05_diff_spend,
  upr_05_diff_spend,
  se_del_mo_amt,
  spend_sig,
  visit_sig
from(
select
  *,
  case
    when lwr_05_diff_txn > 0 then true
    when upr_05_diff_txn < 0 then true
    else false
  end as visit_sig,
  case
    when lwr_05_diff_spend > 0 then true
    when upr_05_diff_spend < 0 then true
    else false
  end as spend_sig
from data
)
order by 1
;


-- roll all categories pre-covid
with
params as(
  select
    date "2020-02-25" as upr_bnd,
    90 as win,
    date "2020-10-31" as legacy_end,
    functions.exc_cht_names(['e']) as exc_cht
),

valid_users as(
  select *
  from `analytics_offers_views.user_segments_vw`
  where ( select count(*) from unnest(cohorts) as cht where cht in unnest((select exc_cht from params)) ) = 0
    and lower(email) not like "%@bumped.com"
    and accountStatus = "approved"
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attr: cat_id
  select * except(dur_sort) from(
  select
    userId as user_id,
    brandId as brand_id,
    brand_name,
    categoryId as category_id,
    cast(selectedAt as date) as selectedAt,
    cast(endedAt as date) as endedAt,
    duration,
    row_number() over(partition by userId, brandId order by duration desc, selectedAt desc) as dur_sort
  from(
    select
      l.userId, l.brandId, b.name as brand_name, l.categoryId,
      timestamp(l.selectedAt) as selectedAt,
      timestamp(l.endedAt) as endedAt,
      date_diff(
        coalesce(date(timestamp(l.endedAt)), current_date),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 as duration
    from `bumped-analytics-aw5325.production.user_loyalty` l
    join `bumped-analytics-aw5325.production.brands` b on(b.id = l.brandId)
    -- gaurds for upper bound and brand
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), least((select legacy_end from params),(select upr_bnd from params)) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
  ))
  where dur_sort = 1
),

users as(
  select
    v.user_id,
    b.category_id,
    min(c.rewardPercentage) as min_rew_pct,
    max(c.rewardPercentage) as max_rew_pct,
    avg(c.rewardPercentage) as avg_rew_pct
  from valid_users v
  cross join unnest(cohorts) as cht
  join analytics_views.brand_campaign_rates_vw c on(c.cohort_name = cht)
  join `analytics_views.brands_vw_2` b on(b.brand_id = c.brand_id)
  group by 1, 2
),

split_txns as(
  -- window and bucket txns for selecting users by category
  select
    t.user_id,
    t.tx_date,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    b.category_id,
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = b.parent_brand_id)
  where
    -- transaction windowing
    date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select win from params))    -- zero not included in pre range so [-n, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select win from params)            -- zero included in post range so [0,n-1]
    and date_diff(current_date, s.selectedAt, day) >= ((select win from params) +3)   -- n days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post dense metrics
  select * from(
    select
      pre_sel,
      user_id,
      category_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3
  )
),

wide_user as(
  select
    *,
    coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0) as del_tot_txn,
    coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) as del_tot_amt,
    coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0) as del_mo_txn,
    coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0) as del_mo_amt,
    safe_divide( coalesce(post_tot_txn,0) - coalesce(pre_tot_txn,0), pre_tot_txn ) as tot_txn_chg, 
    safe_divide( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0), pre_tot_amt ) as tot_amt_chg, 
    safe_divide( ( coalesce(post_tot_amt,0) - coalesce(pre_tot_amt,0) - rew_amt ), rew_amt ) as roi 

  from(
    select
      r.user_id,
      r.category_id,
      -- pre period
      any_value( if(pre_sel, obs, null) ) as pre_tot_txn,
      any_value( if(pre_sel, tot_amt, null) ) as pre_tot_amt,
      any_value( if(pre_sel, avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(pre_sel, avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not pre_sel, obs, null) ) as post_tot_txn,
      any_value( if(not pre_sel, tot_amt, null) ) as post_tot_amt,
      any_value( if(not pre_sel, tot_amt * u.max_rew_pct, null) ) as rew_amt,
      any_value( if(not pre_sel, avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not pre_sel, avg_mo_amt, null) ) as post_avg_amt,

    from user_roll r
    join users u on (u.user_id = r.user_id and u.category_id = r.category_id) -- filter users and bring in reward percent
    group by 1,2
  )
),

data as(
  select
    t.*,
    t.avg_del_amt_mo - ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as lwr_05_diff_spend,
    t.avg_del_amt_mo + ifnull(s.pwr_5, 1.96) * t.se_del_mo_amt as upr_05_diff_spend
  
  from(
  select
    b.name as category_name,
    count(*) as obs,
    countif(w.pre_tot_txn is not null) as pre_users,
    countif(w.post_tot_txn is not null) as post_users,
    countif(w.del_mo_amt > 0) as chg_user_cnt,
    safe_divide(countif(w.del_mo_amt > 0) , countif(w.post_tot_txn is not null)) as pct_chg_users,

    --ticket values
    avg(w.pre_avg_amt / w.pre_avg_txn) as avg_pre_ticket,
    avg(w.post_avg_amt / w.post_avg_txn) as avg_post_ticket,
    -- simple avgerages
    avg(w.pre_avg_txn) as avg_pre_txns_mo,
    avg(w.pre_avg_amt) as avg_pre_amt_mo,
    avg(w.post_avg_txn) as avg_post_txns_mo,
    avg(w.post_avg_amt) as avg_post_amt_mo,
    avg(w.del_mo_txn) as avg_del_txns_mo,
    avg(w.del_mo_amt) as avg_del_amt_mo,
    safe_divide(avg(w.del_mo_txn) , avg(w.pre_avg_txn)) as avg_txn_chg_mo,
    safe_divide(avg(w.del_mo_amt) , avg(w.pre_avg_amt)) as avg_amt_chg_mo,

    -- global top-down values
    safe_divide(sum(w.del_tot_txn) , sum(w.pre_tot_txn)) as top_txn_chg,
    safe_divide(sum(w.del_tot_amt) , sum(w.pre_tot_amt)) as top_amt_chg,
    sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt) as net_rev_chg,
    safe_divide((sum(w.post_tot_amt) - sum(w.pre_tot_amt) - sum(w.rew_amt)) , sum(w.rew_amt)) as roi,

    -- stats
    stddev_samp(w.del_mo_amt) as std_del_mo_amt,
    stddev_samp(w.del_mo_amt) / sqrt(count(*)) as se_del_mo_amt

  from wide_user w
  join `production.brand_categories` b on(b.id = w.category_id)
  where w.post_tot_txn is not null -- make sure users are all shareholders
  group by 1
  ) as t
  left join `bumped-analytics-aw5325.analytics_views.t_stats` s on (t.obs -1 = s.df)
  where
    pre_users >= 20 and post_users >= 20
)

-- output
select
  category_name,
  obs, pre_users, post_users, pct_chg_users,
  avg_pre_txns_mo, avg_post_txns_mo, avg_del_txns_mo,
  avg_pre_amt_mo, avg_post_amt_mo, avg_del_amt_mo,
  
  avg_pre_ticket, avg_post_ticket, avg_post_ticket - avg_pre_ticket as ticket_diff,
  safe_divide(avg_post_ticket - avg_pre_ticket, avg_pre_ticket) as ticket_pct_chg,
  
  avg_txn_chg_mo,
  avg_amt_chg_mo,
  roi,
  lwr_05_diff_spend,
  upr_05_diff_spend,
  se_del_mo_amt,
  spend_sig
from(
select
  *,
  case
    when lwr_05_diff_spend > 0 then true
    when upr_05_diff_spend < 0 then true
    else false
  end as spend_sig
from data
)
order by 1
;



-- make output for slides

select
  "CATEGORY" as type,
  c.category_name as name,
  "90-day" epoch_lvl,
  c.obs,
  c.visit_sig,
  c.spend_sig,
  format('%3.2f', c.avg_del_txns_mo) as diff_visits,
  format('$%0.2f', c.avg_del_amt_mo) as diff_spend,
  format('%0.0f%%', safe_divide(c.post_tot_txns - c.pre_tot_txns, c.pre_tot_txns)) as pct_visit_chg,
  format('%0.0f%%', safe_divide(c.post_tot_amt - c.pre_tot_amt, c.pre_tot_amt)) as pct_spend_chg,
  format('%0.1fx', c.roi + 1) as roi_x,
  format('%0.0f%%', c.pct_chg_users) as pct_user_chg,
  -- null as rew_user_mo
from `marketing_analytics.all_cat_data` c
where spend_sig

union all

select
  "BRAND" as type,
  c.brand_name as name,
  "90-day" epoch_lvl,
  c.obs,
  c.visit_sig,
  c.spend_sig,
  format('%3.2f', c.avg_del_txns_mo) as diff_visits,
  format('$%0.2f', c.avg_del_amt_mo) as diff_spend,
  format('%0.0f%%', safe_divide(c.post_tot_txns - c.pre_tot_txns, c.pre_tot_txns)) as pct_visit_chg,
  format('%0.0f%%', safe_divide(c.post_tot_amt - c.pre_tot_amt, c.pre_tot_amt)) as pct_spend_chg,
  format('%0.1fx', c.roi + 1) as roi_x,
  format('%0.0f%%', c.pct_chg_users) as pct_user_chg,
  -- null as rew_user_mo
from `marketing_analytics.all_brand_data` c
where spend_sig
;