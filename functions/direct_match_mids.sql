create or replace function
functions.direct_match_mids(user string, upr_bnd date) as(

  /*
    direct matching function
    find top brand affinities for a given user
    then map to known mids in affinity order

    input: user string
    returns: an array of result structs (a table)

  */
  
  array(
  with

  user_activity as(
    select
      *, functions.affinity_score_user(tc, ta, sp, lg) as afs
    from(
      select
        atomic_brand_id as brand_id,
        count(*) as tc,
        sum(amount) as ta,
        date_diff(max(tx_date), min(tx_date),day)+1 as sp,
        date_diff(upr_bnd, max(tx_date), day) as lg
      from `analytics_views.tagged_tx`
      where user_id = user
        and tx_date < upr_bnd
      group by 1
    )
    where brand_id is not null
  )

  select
    struct(
      u.brand_id,
      u.afs,
      m.brand_name,
      match,
      row_number() over(order by afs desc) as pos
    ) as rec
  from user_activity u
  join `rec_engine.brand_to_mid_map` m on(m.brand_id = u.brand_id and u.afs > 0)
  cross join m.matches as match
  where match.lev_scr = 1
  order by u.afs desc

  )
);

-- multi function
create or replace function
functions.direct_match_mids_m(u array<string>, upr_bnd date) as(

  /*
    direct matching function
    find top brand affinities for set of users
    then map to known mids in affinity order

    input: array of user strings
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
      from `analytics_views.tagged_tx`
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
  join `rec_engine.brand_to_mid_map` m on(m.brand_id = u.brand_id and u.afs > 0)
  cross join m.matches as match
  left join `rec_engine.global_mid_filter` g on(g.mid = match.mid)
  left join `rec_engine.first_use_vw` f on(f.user_id = u.user_id and f.mid = match.mid)
  where match.lev_scr = 1
    and ( g.mid is null or g.direct ) -- filter mids that are not to be used in direct matches
    and ( not g.first_use or f.user_id is null or not f.hasActivity ) -- remove mids from first-use filter
  order by u.user_id, u.afs desc

  )
);
