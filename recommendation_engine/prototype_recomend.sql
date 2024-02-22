create or replace function
rec_engine.rec_n(users array<string>, upr_bnd date, ret int64) as(
  /*
    rec_engine.rec_n(users array<string>, upr_bnd date, ret int64)
    input: { [userIds], upper_bound, number of recomendations to return (null is all) }
    output: array of recomendation structs (table)

    Algorithm:
      - find top n Plaid brands for user
      - find exact offer matches => store
      - find top atoms for Plaid activity, use to find top n offers with same atoms => store
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
    from unnest( functions.direct_match_mids_m(users, upr_bnd) )
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
    from unnest( functions.atom_match_mids_m(users, upr_bnd) ) a
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
    from unnest( functions.offer_affinity_geo_m(users) ) g
    join `production.merchants` m on(m.__key__.name = g.mid)
    left join dir d on(d.mid = g.mid and d.user_id = g.user_id)
    left join atom a on(a.mid = g.mid and a.user_id = g.user_id)
    where d.mid is null and a.mid is null
  ),

  -- join to active offers and produce output
  out as(
    select
      struct(
        user_id,
        row_number() over(partition by user_id order by t.priority, t.pos) as pos,

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
    join `analytics_offers_views.active_offers_vw` a on(a.mid = t.mid)
  )
  
  select *
  from out
  where rec.pos <= ret or ret is null
  
  )
);
