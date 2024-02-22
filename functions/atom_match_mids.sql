create or replace function
functions.atom_match_mids(user string, upr_bnd date) as(

  /*
    attribute map
    from the users top brands, extract attributes
    from attributes find best mids having those attributes
    
    input: user string and upr_bound date above which no activty is considered
    returns: an array of result structs (a table)

  */

  array(
  with

  user_activity as(
    select * from unnest(functions.user_brand_affinity([user], upr_bnd))
  ),

  top_attr as(
    select atom, sum(afs) as t_afs
    from `rec_engine.manual_brand_atoms` b
    join user_activity u on(u.brand_id = b.brandId and u.afs != 0)
    cross join b.atoms as atom
    group by 1
  ),

  grab_mids as(
    select
      mid,
      any_value(mid_name) as mid_name,
      sum(t_afs) as tt_afs,
      array_agg( struct(atom, t_afs) ) as atom

    from(
      select
        ma.*,
        ta.*
      from `rec_engine.mid_atoms` ma
      cross join top_attr ta
    )
    where atom in unnest(atoms)
    group by 1
  )

  select
    struct(
      mid,
      mid_name,
      tt_afs,
      atom,
      row_number() over(order by tt_afs desc) as pos
    ) as rec
  from grab_mids
  order by tt_afs desc
  )
  
);

-- multi function
create or replace function
functions.atom_match_mids_m(u array<string>, upr_bnd date) as(

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
    select * from unnest(functions.user_brand_affinity(u, upr_bnd))
  ),

  top_attr as(
    select u.user_id, atom, sum(afs) as t_afs
    from `rec_engine.manual_brand_atoms` b
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
      from `rec_engine.mid_atoms` ma
      cross join top_attr ta
    )
    where atom in unnest(atoms)
    group by 1, 2
  )

  select
    struct(
      m.user_id,
      m.mid,
      m.mid_name,
      m.tt_afs,
      m.atom,
      row_number() over(partition by m.user_id order by m.tt_afs desc) as pos
    ) as rec
  from grab_mids m
  left join `rec_engine.global_mid_filter` g on(g.mid = m.mid)
  left join `rec_engine.first_use_vw` f on(f.user_id = m.user_id and f.mid = m.mid)
  where
    (g.mid is null or g.atom) -- remove mids from global filter
    and ( not g.first_use or f.user_id is null or not f.hasActivity ) -- remove mids from first-use filter
  order by m.user_id, tt_afs desc
  )
  
);