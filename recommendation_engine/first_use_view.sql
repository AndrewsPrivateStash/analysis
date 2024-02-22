/*
  find set of users with activity 

*/
create or replace view `rec_engine.first_use_vw` as
with
map as(
  select
    mtb.mid,
    array_agg(match.brand_id) as brands
  from `rec_engine.mid_to_brand_map` mtb
  join `rec_engine.global_mid_filter` g on(g.mid = mtb.mid)
  cross join mtb.matches as match
  where g.first_use 
  group by 1
),

activity as(
  select
    *,
    date_diff(current_date, last_txn, day) as days_since_last
  from(
    select
      t.user_id,
      m.mid,
      max(tx_date) as last_txn,
      any_value(TRUE) as hasActivity
    from `analytics_views.tagged_tx` t
    join map m on (t.brand_id in unnest(m.brands))
    join `production.users` u on (u.id = t.user_id)
    where u.accountStatus = "approved"
    group by 1,2
  )
)

select * from activity
;