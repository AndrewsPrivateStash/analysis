create or replace function
functions.offer_affinity_geo(user string) as(

  /*
    given a user return a list of regional appropriate mids
    based on the offer affinity for those mids in the various scopes
    
    input: user string
    returns: an array of result structs (a table)

  */
  array(
  
  with
  usr as(
    select functions.user_geo([user])[safe_offset(0)] as u_geo
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
    -- union filtered results for user
    select
      *,
      row_number() over(partition by scope.name, scope.value order by afs desc) as scp_pos,
      row_number() over(order by scope.pri desc, afs desc) as glb_pos
    from(
      select struct("glb" as name, "ALL" as value, 0 as pri) as scope, g.mid, g.afs
      from glb_mets g
      where afs > 0

      union all

      select struct("reg" as name, r.region as value, 1 as pri) as scope, r.mid, r.afs
      from reg_mets r
      join usr as u on(u.u_geo.region = r.region)
      where r.afs > 0

      union all

      select struct("div" as name, d.division as value, 2 as pri) as scope, d.mid, d.afs
      from div_mets d
      join usr as u on(u.u_geo.division = d.division)
      where d.afs > 0

      union all

      select struct("state" as name, s.state_code as value, 3 as pri) as scope, s.mid, s.afs
      from state_mets s
      join usr as u on(u.u_geo.state_code = s.state_code)
      where s.afs > 0
    )
  )
  
  select
    struct(
      scope,
      mid,
      afs,
      scp_pos,
      glb_pos
    ) as aff_res
  from out_dat
  order by scope.pri desc, afs desc
  
  )

);


-- multi function
create or replace function
functions.offer_affinity_geo_m(users array<string>) as(

  /*
    given a user return a list of regional appropriate mids
    based on the offer affinity for those mids in the various scopes
    
    input: array of user strings
    returns: an array of result structs (a table)

  */
  array(
  
  with
  usr as(
    select * from unnest(functions.user_geo(users)) as u_geo
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

  combined_dat as(
    select
      *,
      row_number() over(partition by userId, mid order by scope.pri desc) as n
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
  ),

  out_dat as(
    -- union filtered results for users
      select
        t.*,
        row_number() over(partition by scope.name, scope.value order by afs desc) as scp_pos,
        row_number() over(order by scope.pri desc, afs desc) as glb_pos
      from combined_dat t
      left join `rec_engine.global_mid_filter` g on(g.mid = t.mid)
      left join `rec_engine.first_use_vw` f on(f.user_id = t.userId and f.mid = t.mid)
      where
        (g.mid is null or g.geo)
        and ( not g.first_use or f.user_id is null or not f.hasActivity ) -- remove mids from first-use filter
        and n = 1
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
