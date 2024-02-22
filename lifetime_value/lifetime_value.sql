/*
  ####################################
  Bumped Inc.  // proprietary //
  2019-10-25
  Andrew Pfaendler
  andrew.pfaendler@bumped.com

  google bigquery standard SQL
  ####################################

  to show:
  LTV of a Bumped user is x% higher post 90-day versus pre 90-day

  - define LTV rigorously
  LTV {
    assume deminishing effect -- calc from ROI {30, 60, 90}, perp after
    V(t) = ( R(t)[ 1 - C(t) - fr(t) ] - fb(t) ) / ( 1 + wacc ) ^ t
    COGS is likely semi-cosntant --> C
    Reward fee rate is likely semi-cosntant --> fr
    Bumped fee is likely semi-constant --> fb
    ==> R(t)(1 - C - fr) - fb
    if R(t) const, then perpetuity calc ( PV = A / w st. A = R(1-C-fr)-fb )
    with retention effect: DCF: (A * r^t)/(1 + w)^t, Perp: (A * r)/(1 + w - r)
  }
  
  assumptions:
    COGS ~ 50%
    fr = max of camp rates (ROI method)
    fb ~ $0.50 (per selecting user per month)
    wacc = 20%
    
  Method:
  - For set of users/brands/categories:
      - dense monthly avg of 90-day pre-period as perpetuity. (Baseline)
      - Sequence: { 30-day, 60-day, 90-day } post-period dense spend value calc discounted
          - Last period (90-day) used as perp
      - Post series value less pre-period perp value is LTV increase of program for dimension
      - regardless of final dim, base LTV calc done at user dim unless fb is scaled
  
*/

with

params as(
  select
    0.60 as cogs_perc,    -- cost of goods sold percentage, or cost ratio for contribution margin
    0.20 as ann_wacc,     -- weighted average cost of capital (discount rate) {10, 15, 20, 25% ::typical values}
    0.50 as mo_bp_fee,    -- per user monthly Bumped fee
    0.996 as mo_ret_rate, -- monthly retention rate (~5% churn per ann)
    90 as pre_win,        -- span in days for the pre-selection window
    array<INT64>[30, 60, 90] as post_win,  -- upper bounds for post windows, exactly three in ascending order
    30 as post_span,      -- span in days for the post-selection window
    ( select array_agg(cohort) from `analytics_views.exc_chts` ) as exc_chts
    -- ['Alpha','Bravo','Zulu','Graveyard'] as exc_chts  -- cohorts to exclude from study
),

users as(
  -- gross list of users to look at userId:cohort (1:m)
  select u.id as user_id, c.name as cohort_name
  from `bumped-analytics-aw5325.production.users` u
  join `bumped-analytics-aw5325.production.user_cohort_links` ucl on ucl.userId = u.id
  join `bumped-analytics-aw5325.production.cohorts` c on c.id = ucl.cohortId
  where c.name not in unnest( (select exc_chts from params) )  -- filter in-house cohorts
),

loy_sels as(
  -- user selections choosing the longest duration for each user/brand pair with attribute: categoryId
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
        coalesce(cast(timestamp(l.endedAt) as date), current_date),
        cast(timestamp(l.selectedAt) as date),
        day
      ) + 1 as duration
    from `production.user_loyalty` l
    join `production.brands` b on(b.id = l.brandId)
    join users u on(u.user_id = l.userId)
  ))
  where dur_sort = 1
    and duration >= ( select pre_win from params )
),

camp as (
  -- construct reward rates from campaigns by brand / cohort
  select * except(n)
  from(
    select
      brandId as brand_id,
      c.name as cohort_name,
      parse_date('%F', campaigns.startDate) as start_date,
      parse_date('%F', campaigns.endDate) as end_date,
      cast(regexp_extract(campaigns.rewardPercentage, r"([\d\.]+)") as numeric) * 0.01 as rewardPercentage,
      row_number() over (
                    partition by brandId, c.name
                    order by
                      coalesce(parse_date('%F', campaigns.endDate), date '2999-01-01') desc,  -- null wins, otherwise max enddate
                      cast(regexp_extract(campaigns.rewardPercentage, r"([\d\.]+)") as numeric) desc    -- control for dup, choose max rate if conflict
                   ) n
    from `bumped-analytics-aw5325.production.cohort_campaign_links` as links
    join `bumped-analytics-aw5325.production.campaigns` as campaigns on (links.campaignId = campaigns.id)
    join `production.cohorts` c on (c.id = links.cohortId)
  )
  where n = 1 -- remove overlaping/dup reward rows
),

split_txns as(
  -- window and bucket txns
  select
    t.user_id,
    t.tx_date,
    s.category_id,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    t.amount,
    -- manual switch (no need to support arbitrary arrays)
    case
      when date_diff(t.tx_date, s.selectedAt, day) < 0 then "pre"
      when date_diff(t.tx_date, s.selectedAt, day) < (select post_win[offset(0)] from params) then "post_1"
      when date_diff(t.tx_date, s.selectedAt, day) < (select post_win[offset(1)] from params) then "post_2"
      when date_diff(t.tx_date, s.selectedAt, day) < (select post_win[offset(2)] from params) then "post_3"
    end as bucket

  from `analytics_views.tagged_tx` t
  join `analytics_views.brands_vw` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.brand_id = t.brand_id)
  where
    -- transaction windowing
    date_diff(t.tx_date, s.selectedAt, day) >= (-1 * (select pre_win from params))                     -- zero not included in pre range so [-90, -1]
    and date_diff(t.tx_date, s.selectedAt, day) < (select post_win[offset(2)] from params)             -- zero included in post range so [0,89]
    and date_diff(current_date, s.selectedAt, day) >= ((select post_win[offset(2)] from params) +3)    -- 90 days of post activity plus a three day buffer
),

user_roll as(
  -- calc pre and post_n dense metrics
  select * from(
    select
      bucket,
      user_id,
      brand_id,
      category_id,
      
      count(*) as obs,
      -- dense monthly visits
      count(*) /(
          if(bucket = 'pre', (select pre_win from params), (select post_span from params)) * (12/365)
      ) as avg_mo_txns,
      -- dense monthly spend
      sum(amount) /(
          if(bucket = 'pre', (select pre_win from params), (select post_span from params)) * (12/365)
      ) as avg_mo_amt

    from split_txns
    group by bucket, user_id, brand_id, category_id
  )
  where obs > 1
),

wide_user as(
  select * from(
    select
      user_id, brand_id, category_id,

      -- pre metrics
      any_value( if(bucket = 'pre', obs, null) ) as pre_obs,
      any_value( if(bucket = 'pre', avg_mo_txns, null) ) as pre_mo_txns,
      any_value( if(bucket = 'pre', avg_mo_amt, null) ) as pre_mo_amt,
      -- post_1 metrics
      any_value( if(bucket = 'post_1', obs, null) ) as post1_obs,
      any_value( if(bucket = 'post_1', avg_mo_txns, null) ) as post1_mo_txns,
      any_value( if(bucket = 'post_1', avg_mo_amt, null) ) as post1_mo_amt,
      -- post_2 metrics
      any_value( if(bucket = 'post_2', obs, null) ) as post2_obs,
      any_value( if(bucket = 'post_2', avg_mo_txns, null) ) as post2_mo_txns,
      any_value( if(bucket = 'post_2', avg_mo_amt, null) ) as post2_mo_amt,
      -- post_3 metrics
      any_value( if(bucket = 'post_3', obs, null) ) as post3_obs,
      any_value( if(bucket = 'post_3', avg_mo_txns, null) ) as post3_mo_txns,
      any_value( if(bucket = 'post_3', avg_mo_amt, null) ) as post3_mo_amt

    from user_roll
    group by user_id, brand_id, category_id
  )
  where
    pre_obs is not null
    and post1_obs is not null
    and post2_obs is not null
    and post3_obs is not null
),

rew_rate_join as(
  -- join cte to grab the proper reward rate for distinct user/brand ids
  select * except(n) from(
    select
      w.user_id,
      u.cohort_name,
      w.brand_id,
      c.rewardPercentage as rew_rate,
      row_number() over(partition by w.user_id, w.brand_id order by c.rewardPercentage desc) as n

    from wide_user w
    join users u on(u.user_id = w.user_id)
    join camp c on(c.brand_id = w.brand_id and c.cohort_name = u.cohort_name)
  )
  where n = 1  -- force user_id, brand_id dimension with greatest reward rate
),

pre_roll as(
  -- joining reward rates and calculating ltv values by user
  select
    t.*,
    
    -- pre LTV (perpetuity of dense pre-avg with retention effect)
    -- PV = A*r/(1+w-r) st. A = R(1-C)
    (t.pre_mo_amt * (1 - cogs) * ret) / (1 + wacc_mo - ret) as pre_ltv,
    
    -- post LTV
    -- Sum[(V*r^t)/(1+w)^t]_1_2 + (V*r/(1+w-r))*r^t/(1+w)^3  st. V = R(1-C-fr)-fb
    ((t.post1_mo_amt * (1 - cogs - t.rew_rate) - fb) * pow(ret,1)) / pow(1 + wacc_mo,1) +
    ((t.post2_mo_amt * (1 - cogs - t.rew_rate) - fb) * pow(ret,2)) / pow(1 + wacc_mo,2) +
    ((((t.post3_mo_amt * (1 - cogs - t.rew_rate) - fb) * ret) / (1 + wacc_mo - ret)) * pow(ret,3)) / pow(1 + wacc_mo, 3) as post_ltv
    
    /* alt post perp versus last period perp
      think last period perp is correct becuase:
        if monotonically increasing then suggests increases will continue thus under-estimate
        if monotonically decreasing then uses worst known value for perp (but likely over-estimate)
        risk of randomly large 3rd period bump dragged into perp, but reverse is also possible (assume averages out)
          risk of systematic bumps (up or down) due to seasonal effects, other exogenous effects, etc (whole model risks this)
    */

  from(
    select
      w.*,
      r.rew_rate,
      (select cogs_perc from params) as cogs,
      (select mo_bp_fee from params) as fb,
      (select ann_wacc from params)/12.0 as wacc_mo,
      (select mo_ret_rate from params) as ret
    from wide_user w
    join rew_rate_join r on (r.user_id = w.user_id and r.brand_id = w.brand_id)
  ) t
),

r_dump as(
  -- R dump
  select pre_ltv, post_ltv, (post_ltv - pre_ltv) / pre_ltv as rat
  from pre_roll
  where (post_ltv - pre_ltv) / pre_ltv < 100  -- filter extreme cases (four of them, likely illegitimate)
),

cat_roll as(
  select
    -- category summary
    *,
    (sum_post_ltv - sum_pre_ltv) / sum_pre_ltv as diff_ratio
  from(
    select
      category_id,
      count(*) obs,
      sum(pre_ltv) as sum_pre_ltv,
      sum(post_ltv) as sum_post_ltv,
      sum(post_ltv) - sum(pre_ltv) as diff_tot_ltv,
      avg(pre_ltv) as avg_pre_ltv,
      avg(post_ltv) as avg_post_ltv,
      avg(post_ltv - pre_ltv) as avg_diff_ltv

    from pre_roll
    where (post_ltv - pre_ltv) / pre_ltv < 100
    group by category_id
  )
  where obs > 20
  order by category_id
),

brand_roll as(
  select
    -- brand summary
    b.name as brand_name,
    t.* except(brand_id),
    (sum_post_ltv - sum_pre_ltv) / sum_pre_ltv as diff_ratio
  from(
    select
      brand_id,
      count(*) obs,
      sum(pre_ltv) as sum_pre_ltv,
      sum(post_ltv) as sum_post_ltv,
      sum(post_ltv) - sum(pre_ltv) as diff_tot_ltv,
      avg(pre_ltv) as avg_pre_ltv,
      avg(post_ltv) as avg_post_ltv,
      avg(post_ltv - pre_ltv) as avg_diff_ltv

    from pre_roll
    where (post_ltv - pre_ltv) / pre_ltv < 100
    group by brand_id
    
  ) t
  join `production.brands` b on(b.id = t.brand_id)
  where obs > 20
  order by brand_id
),

global_roll as(
  select
    -- global summary
    *,
    (sum_post_ltv - sum_pre_ltv) / sum_pre_ltv as diff_ratio
  from(
    select
      count(*) obs,
      sum(pre_ltv) as sum_pre_ltv,
      sum(post_ltv) as sum_post_ltv,
      sum(post_ltv) - sum(pre_ltv) as diff_tot_ltv,
      avg(pre_ltv) as avg_pre_ltv,
      avg(post_ltv) as avg_post_ltv,
      avg(post_ltv - pre_ltv) as avg_diff_ltv

    from pre_roll
    where (post_ltv - pre_ltv) / pre_ltv < 100
  )
),

demo_roll as(
  select
    -- demo summary
    *,
    (sum_post_ltv - sum_pre_ltv) / sum_pre_ltv as diff_ratio
  from(
    select
      d.generation,
      d.region, 
      count(*) obs,
      sum(p.pre_ltv) as sum_pre_ltv,
      sum(p.post_ltv) as sum_post_ltv,
      sum(p.post_ltv) - sum(pre_ltv) as diff_tot_ltv,
      avg(p.pre_ltv) as avg_pre_ltv,
      avg(p.post_ltv) as avg_post_ltv,
      avg(p.post_ltv - p.pre_ltv) as avg_diff_ltv

    from pre_roll p
    join `analytics_views.user_demo` d on (d.user_id = p.user_id)
    where (p.post_ltv - p.pre_ltv) / p.pre_ltv < 100
    group by 1, 2
  )
  where obs > 25
    and region is not null
  order by 1, 2
)

select * from brand_roll
--select * from global_roll
--select * from cat_roll
--select * from r_dump
