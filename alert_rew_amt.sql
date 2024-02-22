/*
  suspicious activity Alert
  
    - flag any reward >= $50.00
    
    => flag txn write to exception table
    => dump to CSV in drive
    => email internal with CSV

*/

with
params as(
  select
    4.0 as std_devs,
    180 as lookback_win,
    5 as inspection_win,
    25.00 as flat_thr,
    25 as stat_obs_min,
    20 as stat_user_min,
),

exc_users as(
  select
    id as user_id,
    externalApexId
  from `production.users`
  where accountStatus in("frozen", "deactivated")
),

rew_activity as(
  -- rewards in lookback period
  select
    *,
    log( if(rew_amt < 0, NULL, rew_amt)) as log_rew_amt
  from(
  select
    id as rew_id,
    transactionId as txn_id,
    timestamp(createdAt) as stamp,
    userId as user_id,
    triggerType as trigger_type,
    companyId,
    cast(regexp_extract(amount, r"(\-?[0-9\.]+)") as numeric) as rew_amt
  from production.rewards 
  where date(timestamp(createdAt)) >= date_sub(current_date, interval (select lookback_win from params) + (select inspection_win from params) day)
    and date(timestamp(createdAt)) < date_sub(current_date, interval (select inspection_win from params) day)
    and journalDate is not null
    and status not in("ineligible", "unprocessable", "pendingReview", "unprocessed", "errored")
    and userId not in( select user_id from exc_users )
    and status not in ("ineligible", "unprocessable")
  )
),

rew_mets as(
  select
    trigger_type,
    companyId,
    count(*) as obs,
    count(distinct user_id) as user_cnt,
    sum(rew_amt) as sum_rew,
    avg(rew_amt) as avg_rew,
    approx_quantiles(rew_amt, 10)[offset(5)] as med_rew,
    stddev_samp(rew_amt) as std_rew,
    -- log normal
    avg(log_rew_amt) as avg_log_rew,
    stddev_samp(log_rew_amt) as std_log_rew,
    avg(log_rew_amt) + ( select std_devs from params ) * stddev_samp(log_rew_amt) as upr_log_thr
  from rew_activity
  group by 1, 2
),

flag_rew_act as (
  select
    u.externalApexId as apex_id,
    r.*,
    log(r.rew_amt) as log_rew_amt,
    m.avg_log_rew,
    m.std_log_rew,
    m.upr_log_thr,
    safe_divide( log(r.rew_amt) - m.avg_log_rew, m.std_log_rew ) as stdvs_over
  from(
    select
      id as rew_id,
      transactionId as txn_id,
      timestamp(createdAt) as stamp,
      userId as user_id,
      triggerType as trigger_type,
      companyId,
      cast(regexp_extract(amount, r"(\-?[0-9\.]+)") as numeric) as rew_amt,
    from `production.rewards`
    where cast(regexp_extract(amount, r"(\-?[0-9\.]+)") as numeric) > 0
      and status not in ("ineligible", "unprocessable")
  ) as r
  join rew_mets m on (m.trigger_type = r.trigger_type and m.companyId = r.companyId)
  join `production.users` u on (u.id = r.user_id)
  where date(stamp) >= date_sub(current_date, interval (select inspection_win from params) day)
    and ( log(r.rew_amt) > upr_log_thr or r.rew_amt >= (select flat_thr from params) )    -- over stat thr, or over flat thr
    and ( user_id not in( select user_id from exc_users ) or date(stamp) = current_date )   -- not canceled user unless it's current date
    and m.obs >= ( select stat_obs_min from params )
    and m.user_cnt >= ( select stat_user_min from params )
)

select *
from flag_rew_act
order by stdvs_over desc
