create or replace function ops.user_touch_ips(u array<string>, a array<string>) as (
/*
    input: [userId] || [ExternalApexIds]
    returns: array of structs (table)
             ids, acct_status, last_context, last_ip, last_touch, {apex}, {ips}, {contexts}
             
    -- look up both UserIds and ApexIds
    select * from unnest(ops.user_touch_ips(["a77b849a-f5bf-4ee4-ac46-027768a65863"], ["3BP0####", "3BP1####"]));

    -- look up only UserIds
    select * from unnest(ops.user_touch_ips(["a77b849a-f5bf-4ee4-ac46-027768a65863"], []));

    -- look up only ApexIds
    select * from unnest(ops.user_touch_ips([], ["3BP0####", "3BP1####"]));
*/
array(
  with  
  dat as(
    select *
    from `analytics_views.raw_user_activity`
    where user_id in unnest(u)
      or externalApexId in unnest(a)
  ),
  
  last_ip as(
    select
      user_id,
      context_ip as last_ip
    from dat
    where context_ip is not null
    qualify row_number() over(partition by user_id order by original_timestamp desc ) = 1
  ),
  
  all_ips as(
    select
      t.*,
      lip.last_ip
    from(
      select
        user_id,
        array_agg(
          struct(
            context_ip as address,
            cnt as cnt,
            fst as first_used,
            lst as last_used
          )
        ) as ip

      from(
        select
          user_id,
          context_ip,
          count(*) as cnt,
          min(original_timestamp) as fst,
          max(original_timestamp) as lst
        from dat
        where context_ip is not null
        group by 1, 2
        order by 3 desc, 5 desc
      ) 
      group by 1
    ) t
    join last_ip lip on(lip.user_id = t.user_id)
  ),
  
  all_contexts as(
    select
      user_id,
      array_agg(
        struct(
          source_table as name,
          cnt as cnt,
          fst as first_used,
          lst as last_used
        )
      ) as context
      
    from(
      select
        user_id,
        source_table,
        count(*) as cnt,
        min(original_timestamp) as fst,
        max(original_timestamp) as lst
      from dat
      group by 1, 2
      order by 3 desc, 5 desc
    )
    group by 1
  ),
  
  last_touch as(
    select *
    from dat
    qualify row_number() over(partition by user_id order by original_timestamp desc ) = 1
  ),
  
  apex as(
    select  
      account_number,
      struct(
        open_date,
        last_change_date,
        last_activity_date,
        closed_date
      ) as apex
    from `apex.accounts`
    qualify row_number() over(partition by account_number order by file_date desc) = 1
  )
  
  select
    struct(
      lt.externalApexId,
      lt.user_id,
      u.accountStatus,
      lt.source_table as last_context,
      lt.context_ip as last_touch_ip,
      lt.original_timestamp as last_touch_stamp,
      aips.last_ip,
      a.apex,
      aips.ip,
      acon.context
    )
  from last_touch lt
  join `production.users` u on(u.id = lt.user_id)
  left join all_ips aips on(aips.user_id = lt.user_id)
  left join all_contexts acon on(acon.user_id = lt.user_id)
  left join apex a on (a.account_number = lt.externalApexId)
))
;