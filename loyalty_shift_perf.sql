/*
  Given set of brands, determine the loyalty shift between them for those users selecting the brands
  That is, if a user selects brand A, what is that users response to brand B, relative to the brand A response?
  
  -- COVID cut date: 2020-02-25
  -- offer start date: 2020-09-29
  -- end of legacy rewards date: 2020-10-31

*/

with
params as(
  select
    array<string>["client1", "client2"] as brand_ids,
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

split_txns as(
  -- window and bucket txns for selecting users by category
  -- limit results to brands in brand_ids array (assumes they have common category)
  select
    t.user_id,
    t.tx_date,
    s.category_id,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    s.brand_id as sels_brand,
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.category_id = b.category_id)
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
      category_id,
      sels_brand,
      brand_id,
      
      count(*) as obs,
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3,4,5
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
    safe_divide( coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0), pre_avg_txn ) as txn_chg_mo, 
    safe_divide( coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0), pre_avg_amt ) as amt_chg_mo,
      
  from(
    select
      r.user_id,
      r.category_id,
      r.brand_id,
      r.sels_brand,

      -- pre period
      any_value( if(r.pre_sel, r.obs, null) ) as pre_tot_txn,
      any_value( if(r.pre_sel, r.tot_amt, null) ) as pre_tot_amt,
      any_value( if(r.pre_sel, r.avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(r.pre_sel, r.avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not r.pre_sel, r.obs, null) ) as post_tot_txn,
      any_value( if(not r.pre_sel, r.tot_amt, null) ) as post_tot_amt,
      any_value( if(not r.pre_sel, r.avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not r.pre_sel, r.avg_mo_amt, null) ) as post_avg_amt

    from user_roll r
    join valid_users u on(u.user_id = r.user_id) -- filter users
    group by 1,2,3,4
  )
),

pre_out as(
select * except(n) from(
  select
    *,
    -- second diff
    sel_del_txn - unsel_del_txn as sel_v_unsel_txn,
    sel_del_amt - unsel_del_amt as sel_v_unsel_amt,
    safe_divide(sel_del_txn - unsel_del_txn, unsel_del_txn) as sel_txn_pct_diff,
    safe_divide(sel_del_amt - unsel_del_amt, unsel_del_amt) as sel_amt_pct_diff,
    
    -- wallet change
    sel_wallet_share_post - sel_wallet_share_pre as sel_wallet_diff,
    
    row_number() over(partition by user_id, category_id order by sel_del_amt - unsel_del_amt desc) as n -- if multiples for a category choose best response

  from(
    select
      *,
      -- first diffs
      coalesce(sel_post_tot_txn,0) - coalesce(sel_pre_tot_txn,0) as sel_del_txn,
      coalesce(sel_post_tot_amt,0) - coalesce(sel_pre_tot_amt,0) as sel_del_amt,
      coalesce(unsel_post_tot_txn,0) - coalesce(unsel_pre_tot_txn,0) as unsel_del_txn,
      coalesce(unsel_post_tot_amt,0) - coalesce(unsel_pre_tot_amt,0) as unsel_del_amt,
      
      -- wallet metrics
      coalesce(sel_pre_tot_amt,0) + coalesce(unsel_pre_tot_amt,0) as pre_wallet,
      coalesce(sel_post_tot_amt,0) + coalesce(unsel_post_tot_amt,0) as post_wallet,
      
      safe_divide(coalesce(sel_pre_tot_amt,0), coalesce(sel_pre_tot_amt,0) + coalesce(unsel_pre_tot_amt,0)) as sel_wallet_share_pre,
      safe_divide(coalesce(sel_post_tot_amt,0), coalesce(sel_post_tot_amt,0) + coalesce(unsel_post_tot_amt,0)) as sel_wallet_share_post,

    from(
      select
        w.user_id,
        w.category_id,
        w.sels_brand,
        string_agg(if( w.brand_id != w.sels_brand, w.brand_id, null )) as unsel_brands,

        any_value( if( w.brand_id = w.sels_brand, w.pre_tot_txn, null) ) as sel_pre_tot_txn,
        any_value( if( w.brand_id = w.sels_brand, w.pre_tot_amt, null) ) as sel_pre_tot_amt,
        any_value( if( w.brand_id = w.sels_brand, w.post_tot_txn, null) ) as sel_post_tot_txn,
        any_value( if( w.brand_id = w.sels_brand, w.post_tot_amt, null) ) as sel_post_tot_amt,

        sum( if( w.brand_id != w.sels_brand, w.pre_tot_txn, null) ) as unsel_pre_tot_txn,
        sum( if( w.brand_id != w.sels_brand, w.pre_tot_amt, null) ) as unsel_pre_tot_amt,
        sum( if( w.brand_id != w.sels_brand, w.post_tot_txn, null) ) as unsel_post_tot_txn,
        sum( if( w.brand_id != w.sels_brand, w.post_tot_amt, null) ) as unsel_post_tot_amt

      from wide_user w
      where
      -- arbitrary assumptions
        true
      group by 1,2,3
    )
  ))
  where n = 1
    and ( unsel_pre_tot_txn is not null or unsel_post_tot_txn is not null ) -- make sure there is something to compare
)


/* output */
-- look at % of wallet shift
select
  sels_brand,
  unsel_brands,
  
  sum(sel_post_tot_amt) / sum(post_wallet) - sum(sel_pre_tot_amt) / sum(pre_wallet) as sel_wallet_chg,

  sum(sel_del_amt) as sel_amt_diff,
  sum(unsel_del_amt) as unsel_amt_diff,
  sum(sel_del_amt) - sum(unsel_del_amt) as sel_v_unsel_amt,
  
  avg(sel_v_unsel_amt) as avg_sel_v_unsel,
  safe_divide(sum(sel_v_unsel_amt) , sum(unsel_del_amt)) as sel_v_unsel_pct

from pre_out
group by 1,2
;


-- look instead at all such pairs posible
/*
  determine the loyalty shift between all brand pairs sharing a common category in terms of wallet share shift
  That is, if a user selects brand A, what is that users response to brand B, relative to the brand A response in terms of % of spend?
  
  -- COVID cut date: 2020-02-25
  -- offer start date: 2020-09-29
  -- end of legacy rewards date: 2020-10-31

*/

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
    -- gaurds for upper bound
    where
      date_diff(
        coalesce( date(timestamp(l.endedAt)), least((select legacy_end from params),(select upr_bnd from params)) ),
        date(timestamp(l.selectedAt)),
        day
      ) + 1 >= ( select win from params )
  ))
  where dur_sort = 1
),

split_txns as(
  -- window and bucket txns for selecting users by category
  -- limit results to brands in brand_ids array (assumes they have common category)
  select
    t.user_id,
    t.tx_date,
    b.category_id,
    b.parent_brand_id as brand_id,  -- promote brand to parent as parent is selected not child
    s.brand_id as sels_brand,
    t.amount,
    date_diff(t.tx_date, s.selectedAt, day) < 0 as pre_sel
    
  from `bumped-analytics-aw5325.analytics_views.tagged_tx` t
  join `bumped-analytics-aw5325.analytics_views.brands_vw_2` b on (b.brand_id = t.brand_id)
  join loy_sels s on (s.user_id = t.user_id and s.category_id = b.category_id)
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
      sels_brand,
      brand_id,
      
      count(*) as obs,  -- also transaction count
      sum(amount) as tot_amt,
      count(*) / ( select win from params ) * 365/12  as avg_mo_txns,  -- dense avg
      sum(amount) / ( select win from params ) * 365/12 as avg_mo_amt

    from split_txns s
    group by 1,2,3,4,5
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
    safe_divide( coalesce(post_avg_txn,0) - coalesce(pre_avg_txn,0), pre_avg_txn ) as txn_chg_mo, 
    safe_divide( coalesce(post_avg_amt,0) - coalesce(pre_avg_amt,0), pre_avg_amt ) as amt_chg_mo,
      
  from(
    select
      r.user_id,
      r.category_id,
      r.brand_id,
      r.sels_brand,

      -- pre period
      any_value( if(r.pre_sel, r.obs, null) ) as pre_tot_txn,
      any_value( if(r.pre_sel, r.tot_amt, null) ) as pre_tot_amt,
      any_value( if(r.pre_sel, r.avg_mo_txns, null) ) as pre_avg_txn,
      any_value( if(r.pre_sel, r.avg_mo_amt, null) ) as pre_avg_amt,
      -- post period
      any_value( if(not r.pre_sel, r.obs, null) ) as post_tot_txn,
      any_value( if(not r.pre_sel, r.tot_amt, null) ) as post_tot_amt,
      any_value( if(not r.pre_sel, r.avg_mo_txns, null) ) as post_avg_txn,
      any_value( if(not r.pre_sel, r.avg_mo_amt, null) ) as post_avg_amt

    from user_roll r
    join valid_users u on(u.user_id = r.user_id) -- filter users
    group by 1,2,3,4
  )
),

pre_out as(
select * from(
  select
    *,
    -- second diff
    sel_del_txn - unsel_del_txn as sel_v_unsel_txn,
    sel_del_amt - unsel_del_amt as sel_v_unsel_amt,
    safe_divide(sel_del_txn - unsel_del_txn, unsel_del_txn) as sel_txn_pct_diff,
    safe_divide(sel_del_amt - unsel_del_amt, unsel_del_amt) as sel_amt_pct_diff,
    
    -- wallet change
    sel_wallet_share_post - sel_wallet_share_pre as sel_wallet_diff,
    
  from(
    select
      *,
      -- first diffs
      coalesce(sel_post_tot_txn,0) - coalesce(sel_pre_tot_txn,0) as sel_del_txn,
      coalesce(sel_post_tot_amt,0) - coalesce(sel_pre_tot_amt,0) as sel_del_amt,
      coalesce(unsel_post_tot_txn,0) - coalesce(unsel_pre_tot_txn,0) as unsel_del_txn,
      coalesce(unsel_post_tot_amt,0) - coalesce(unsel_pre_tot_amt,0) as unsel_del_amt,
      
      -- wallet metrics
      coalesce(sel_pre_tot_amt,0) + coalesce(unsel_pre_tot_amt,0) as pre_wallet,
      coalesce(sel_post_tot_amt,0) + coalesce(unsel_post_tot_amt,0) as post_wallet,
      
      safe_divide(coalesce(sel_pre_tot_amt,0), coalesce(sel_pre_tot_amt,0) + coalesce(unsel_pre_tot_amt,0)) as sel_wallet_share_pre,
      safe_divide(coalesce(sel_post_tot_amt,0), coalesce(sel_post_tot_amt,0) + coalesce(unsel_post_tot_amt,0)) as sel_wallet_share_post,

    from(
      select
        w.user_id,
        w.category_id,
        b_sel.name as sel_brand_name,
        string_agg(if( w.brand_id != w.sels_brand, b_bnd.name, null )) as unsel_brands,

        any_value( if( w.brand_id = w.sels_brand, w.pre_tot_txn, null) ) as sel_pre_tot_txn,
        any_value( if( w.brand_id = w.sels_brand, w.pre_tot_amt, null) ) as sel_pre_tot_amt,
        any_value( if( w.brand_id = w.sels_brand, w.post_tot_txn, null) ) as sel_post_tot_txn,
        any_value( if( w.brand_id = w.sels_brand, w.post_tot_amt, null) ) as sel_post_tot_amt,

        sum( if( w.brand_id != w.sels_brand, w.pre_tot_txn, null) ) as unsel_pre_tot_txn,
        sum( if( w.brand_id != w.sels_brand, w.pre_tot_amt, null) ) as unsel_pre_tot_amt,
        sum( if( w.brand_id != w.sels_brand, w.post_tot_txn, null) ) as unsel_post_tot_txn,
        sum( if( w.brand_id != w.sels_brand, w.post_tot_amt, null) ) as unsel_post_tot_amt

      from wide_user w
      join `production.brands` b_sel on (b_sel.id = w.sels_brand)
      join `production.brands` b_bnd on (b_bnd.id = w.brand_id)
      where
      -- arbitrary assumptions
        true
      group by 1,2,3
    )
  ))
  where ( unsel_pre_tot_txn is not null or unsel_post_tot_txn is not null ) -- make sure there is something to compare
)

-- output
-- look at % of wallet shift
select
  sel_brand_name,
  -- remove any dups in the array and convert to string list
  array_to_string(( select array_agg(distinct u) from unnest(unsel_brands) u ), ",") as unsel_brands,
  * except(sel_brand_name, unsel_brands)

from(
  select
    sel_brand_name,

    count(*) as obs,
    array_concat_agg(unsel_brands) as unsel_brands, --roll up all the unsel brands (not distinct)
    string_agg(distinct category_id) as categories,

    sum(sel_post_tot_amt) / sum(post_wallet) - sum(sel_pre_tot_amt) / sum(pre_wallet) as sel_wallet_chg,

    sum(sel_del_amt) as sel_amt_diff,
    sum(unsel_del_amt) as unsel_amt_diff,
    sum(sel_del_amt) - sum(unsel_del_amt) as sel_v_unsel_amt,

    avg(sel_v_unsel_amt) as avg_sel_v_unsel,
    case
      when sum(unsel_del_amt) < 0 then null
      else safe_divide(sum(sel_v_unsel_amt) , sum(unsel_del_amt))
    end as sel_v_unsel_pct,

  from ( select * except(unsel_brands), split(unsel_brands) as unsel_brands from pre_out )
  group by 1
)
where sel_wallet_chg is not null
  and obs >= 25
order by sel_wallet_chg desc
;