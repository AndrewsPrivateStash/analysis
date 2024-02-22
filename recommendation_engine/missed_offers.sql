/*
  "oh noes you could have gotten stonks"
  from user activity data, look at active offers and return set of offers:
    where
      not clicked on
      offer is active
      user made purchase at brand in recent past

*/

create or replace function
rec_engine.missed_offers(users array<string>, lwr_bnd date, upr_bnd date, span int64) as(

  /*
    rec_engine.missed_offers(users array<string>, lwr_bnd date, upr_bnd date, span int64)
    
    "oh noes you could have gotten stonks"
    from user activity data, look at active offers and return set of offers not clicked on
    find top brand affinities for set of users
    then map to known mids in affinity order

    input: array of user strings, lower bound date (inc), upper bound date (exc), span in days to transact after offer press
    returns: an array of result structs (a table)
    
    upr_bnd - lwr_bnd defined the window which a txn must occur from the press event

  */
  
  array(
  
  with 
  user_activity as(
    select
      user_id,
      atomic_brand_id as brand_id,
      count(*) as tc,
      sum(amount) as ta,
      max(tx_date) as max_date,
      min(tx_date) as min_date,  -- use this for bounds checking
      date_diff(max(tx_date), min(tx_date),day)+1 as sp,
      date_diff(upr_bnd, max(tx_date), day) as lg
    from `analytics_views.tagged_tx`
    where user_id in unnest(users)
      and tx_date >= lwr_bnd
      and tx_date < upr_bnd
      and atomic_brand_id is not null
    group by 1, 2
  ),
  
  act_mids as(
    select
        u.user_id,
        u.brand_id,
        m.brand_name,
        u.min_date, -- first date in period of txn
        u.tc,
        u.ta,
        u.sp,
        u.lg,
        match
    from user_activity u
    join `rec_engine.brand_to_mid_map` m on(m.brand_id = u.brand_id)
    cross join m.matches as match
    left join `rec_engine.global_mid_filter` g on(g.mid = match.mid)
    where match.lev_scr = 1
      and ( g.mid is null or g.missed ) -- filter mids that are not to be used in missed matches
  ),
  
  viable_mids as(
    select * from(
    select
      *, 
      logical_or(exclude_mid) over(partition by user_id, match.mid) as glb_exlude,
      row_number() over(partition by user_id, match.mid) as n
    from(
      select
        a.*,
        a.match.mid in unnest(m.mids)
          and(
            -- an offer was pressed or activated inside the window
            --( m.type in("CLO_ACT", "SHOP-NOW") and date(m.timestamp) >= lwr_bnd and date(m.timestamp) < upr_bnd )

            -- the press to txn lag is inside the window
            ( m.type in("CLO_ACT", "SHOP-NOW") and abs( date_diff(a.min_date, date(m.timestamp), day) ) < span )
            -- an active loyalty was selected inside, or before the window
            or m.type = "LOYALTY" and date(m.timestamp) < upr_bnd
          ) as exclude_mid
      from act_mids a
      left join analytics_offers_views.offer_press_merchants m on(m.user_id = a.user_id and a.match.mid in unnest(m.mids))
      left join `rec_engine.first_use_vw` f on(f.user_id = a.user_id and f.mid = a.match.mid)
      where not f.hasActivity or f.user_id is null
    ))
    where not glb_exlude and n = 1
  ),
  
  missed_offers as(
    select *, row_number() over(partition by user_id order by v.ta desc) as pos
    from viable_mids v
    join `analytics_offers_views.active_offers_vw` a on(a.mid = v.match.mid)
  )
  
  select
    struct(
      user_id,
      pos,
      brand_id,
      brand_name,
      mid as merchant,
      merchant_title,
      offer_id,
      aggregator,
      aggregator_offer_id,
      url,
      revision_number
    ) as rec
  from missed_offers
  
))
;