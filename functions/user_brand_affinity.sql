create or replace function
functions.user_brand_affinity(user array<string>, upr_bnd date) as(

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
      from `analytics_views.tagged_tx`
      where user_id in unnest(user)
        and tx_date < upr_bnd
      group by 1, 2
    )
    where brand_id is not null
  )
 
);