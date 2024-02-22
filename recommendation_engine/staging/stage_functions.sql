
-- direct_match_mids_m
create or replace function
rec_engine_staging.direct_match_mids_m(u array<string>, upr_bnd date) as(

  /*
    direct matching function
    find top brand affinities for set of users
    then map to known mids in affinity order

    input: array of user strings, upper bound date
    returns: an array of result structs (a table)

  */
  
  array(
  with

  user_activity as(
    select
      *, functions.affinity_score_user(tc, ta, sp, lg) as afs
    from(
      select
        user_id,
        atomic_brand_id as brand_id,
        count(*) as tc,
        sum(amount) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        date_diff(upr_bnd, max(tx_date), day) as lg
      from `rec_engine_staging.tagged_tx`
      where user_id in unnest(u)
        and tx_date < upr_bnd
      group by 1, 2
    )
    where brand_id is not null
  )

  select
    struct(
      u.user_id,
      u.brand_id,
      u.afs,
      m.brand_name,
      match,
      row_number() over(order by afs desc) as pos
    ) as rec
  from user_activity u
  join `rec_engine_staging.brand_to_mid_map` m on(m.brand_id = u.brand_id and u.afs > 0)
  cross join m.matches as match
  where match.lev_scr = 1
  order by user_id, u.afs desc

  )
);


-- user_brand_affinity
create or replace function
rec_engine_staging.user_brand_affinity(user array<string>, upr_bnd date) as(

  /*
    find top brand affinities for a given user

    input: user string and upr_bound date above which no activty is considered
    returns: an array of result structs (a table)
      { brand, total_txn_count, total_txn_amount, span in days between first and last txn}

  */
 
  array(
  
    select
      struct(
        user_id,
        brand_id,
        tc, ta, sp, lg,
        functions.affinity_score_user(tc, ta, sp, lg) as afs
      ) as rec
    from(
      select
        user_id,
        brand_id,
        count(*) as tc,
        sum(amount) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        date_diff(upr_bnd, max(tx_date),day) as lg
      from `rec_engine_staging.tagged_tx`
      where user_id in unnest(user)
        and tx_date < upr_bnd
      group by 1, 2
    )
    where brand_id is not null
  )
 
);


-- atom_match_mids_m
create or replace function
rec_engine_staging.atom_match_mids_m(u array<string>, upr_bnd date) as(

  /*
    attribute map
    from the users top brands, extract attributes
    from attributes find best mids having those attributes
    
    input: array of user strings and upr_bound date above which no activty is considered
    returns: an array of result structs (a table)

  */

  array(
  with

  user_activity as(
    select * from unnest(rec_engine_staging.user_brand_affinity(u, upr_bnd))
  ),

  top_attr as(
    select u.user_id, atom, sum(afs) as t_afs
    from `rec_engine_staging.manual_brand_atoms` b
    join user_activity u on(u.brand_id = b.brandId and u.afs != 0)
    cross join b.atoms as atom
    group by 1, 2
  ),

  grab_mids as(
    select
      user_id,
      mid,
      any_value(mid_name) as mid_name,
      sum(t_afs) as tt_afs,
      array_agg( struct(atom, t_afs) ) as atom

    from(
      select
        ma.*,
        ta.*
      from `rec_engine_staging.mid_atoms` ma
      cross join top_attr ta
    )
    where atom in unnest(atoms)
    group by 1, 2
  )

  select
    struct(
      user_id,
      mid,
      mid_name,
      tt_afs,
      atom,
      row_number() over(partition by user_id order by tt_afs desc) as pos
    ) as rec
  from grab_mids
  order by user_id, tt_afs desc
  )
  
);

-- user_geo
create or replace function
rec_engine_staging.user_geo(a array<string>) as(
  /*
    grab a set of users geo data
    input: an array of userIds
    returns: an array of geo structs
  */
  array(
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
      where userId in unnest( a )
        and type = 'HOME'
    ) t
    left join `bigquery-public-data.utility_us.zipcode_area` za on(za.zipcode = t.postalCode)
    left join `bumped-analytics-aw5325.analytics_views.state_dim` sd on(sd.state_code = coalesce(za.state_code, t.state))
    where n = 1 -- ensure user dim
 )
);


-- offer_affinity_geo_m
create or replace function
rec_engine_staging.offer_affinity_geo_m(users array<string>) as(

  /*
    given a user return a list of regional appropriate mids
    based on the offer affinity for those mids in the various scopes
    
    input: array of user strings
    returns: an array of result structs (a table)

  */
  array(
  
  with
  usr as(
    select * from unnest(rec_engine_staging.user_geo(users)) as u_geo
  ),

  dat as(
    select
      date(a.transactionDate) as tx_date,
      a.user_id,
      a.offerId,
      a.mid,
      a.merchantDisplayName,
      a.qualified_txn_amt,
      a.rebate_amt,
      g.geo.state_code,
      g.geo.division,
      g.geo.region
    from `rec_engine_staging.accrued_rewards_vw` a
    join `rec_engine_staging.user_geo_vw` g on(g.geo.userId = a.user_id)
    where a.qualified_txn_amt > 0
    
    -- augment sparse data
    union distinct
    
    select
      date(a.transactionDate) as tx_date,
      a.user_id,
      a.offerId,
      a.mid,
      a.merchantDisplayName,
      a.qualified_txn_amt,
      a.rebate_amt,
      g.geo.state_code,
      g.geo.division,
      g.geo.region
    from `analytics_offers_views.accrued_rewards_vw` a
    join `analytics_views.user_geo_vw` g on(g.geo.userId = a.user_id)
    where a.qualified_txn_amt > 0
  ),

  glb_mets as(
    select
      *, functions.affinity_score(tc, ta, sp, uc) as afs
    from(
      select
        mid,
        count(*) as tc,
        sum(qualified_txn_amt) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        count(distinct user_id) as uc
      from dat
      group by 1
    )
    where mid is not null
  ),

  reg_mets as(
    select
      *, functions.affinity_score(tc, ta, sp, uc) as afs
    from(
      select
        mid,
        region,
        count(*) as tc,
        sum(qualified_txn_amt) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        count(distinct user_id) as uc
      from dat
      group by 1,2
    )
    where mid is not null
      and region is not null
  ),

  div_mets as(
    select
      *, functions.affinity_score(tc, ta, sp, uc) as afs
    from(
      select
        mid,
        division,
        count(*) as tc,
        sum(qualified_txn_amt) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        count(distinct user_id) as uc
      from dat
      group by 1,2
    )
    where mid is not null
      and division is not null
  ),

  state_mets as(
    select
      *, functions.affinity_score(tc, ta, sp, uc) as afs
    from(
      select
        mid,
        state_code,
        count(*) as tc,
        sum(qualified_txn_amt) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        count(distinct user_id) as uc
      from dat
      group by 1,2
    )
    where mid is not null
      and state_code is not null
  ),

  out_dat as(
    -- union filtered results for users
    select
      *,
      row_number() over(partition by scope.name, scope.value order by afs desc) as scp_pos,
      row_number() over(order by scope.pri desc, afs desc) as glb_pos
    from(
      select u.userId, struct("glb" as name, "ALL" as value, 0 as pri) as scope, g.mid, g.afs
      from glb_mets g
      cross join usr u
      where afs > 0

      union all

      select u.userId, struct("reg" as name, r.region as value, 1 as pri) as scope, r.mid, r.afs
      from reg_mets r
      join usr as u on(u.region = r.region)
      where r.afs > 0

      union all

      select u.userId, struct("div" as name, d.division as value, 2 as pri) as scope, d.mid, d.afs
      from div_mets d
      join usr as u on(u.division = d.division)
      where d.afs > 0

      union all

      select u.userId, struct("state" as name, s.state_code as value, 3 as pri) as scope, s.mid, s.afs
      from state_mets s
      join usr as u on(u.state_code = s.state_code)
      where s.afs > 0
    )
  )
  
  select
    struct(
      userId as user_id,
      scope,
      mid,
      afs,
      scp_pos,
      glb_pos
    ) as aff_res
  from out_dat
  order by userId, scope.pri desc, afs desc
  
  )

);


-- rec_n
create or replace function
rec_engine_staging.rec_n(users array<string>, upr_bnd date, ret int64) as(
  /*
    Algorithm:
      input: { userId, number of recomendations to return (null is all) }
      output: array of recomendation structs (table)

      - find top n [provider] brands for user
      - find exact offer matches => store
      - find top atoms for [provider] activity, use to find top n offers with same atoms => store
      - find top offers regionally (all users in userId's region) => store

      fill recomend count in order (direct, atom match, region-top)
      for locallity, criterea ladder from: city -> state -> division -> region -> global

  */
  array(
  with
  -- direct matches
  dir as(
    select
      user_id,
      0 as priority,
      "direct" as rec_type,
      match.mid as mid,
      match.mid_name as mid_name,
      pos
    from unnest( rec_engine_staging.direct_match_mids_m(users, upr_bnd) )
  ),

  -- atom matches
  atom as(
    select
      a.user_id,
      1 as priority,
      "atom" as rec_type,
      a.mid,
      a.mid_name,
      a.pos
    from unnest( rec_engine_staging.atom_match_mids_m(users, upr_bnd) ) a
    left join dir d on(d.mid = a.mid and d.user_id = a.user_id)
    where d.mid is null
  ),

  -- geo matches
  geo as(
    select
      g.user_id,
      2 as priority,
      "geo" as rec_type,
      g.mid,
      m.title as mid_name,
      g.glb_pos as pos
    from unnest( rec_engine_staging.offer_affinity_geo_m(users) ) g
    join `staging.merchants` m on(m.__key__.name = g.mid)
    left join dir d on(d.mid = g.mid and d.user_id = g.user_id)
    left join atom a on(a.mid = g.mid and a.user_id = g.user_id)
    where d.mid is null and a.mid is null
  ),

  -- join to active offers and produce output
  out as(
    select
      struct(
        user_id,
        row_number() over(partition by user_id order by priority, pos) as pos,

        t.rec_type,

        struct(
          a.offer_id,
          a.mid as merchant_id,
          a.title,
          a.merchant_title,
          a.aggregator,
          a.aggregator_offer_id,
          a.url,
          a.revision_number
        ) as offer
      ) as rec
    from(
      select * from dir
      union all
      select * from atom
      union all
      select * from geo
    ) t
    join `rec_engine_staging.active_offers_vw` a on(a.mid = t.mid)
  )
  
  select *
  from out
  where rec.pos <= ret or ret is null
  
  )
);