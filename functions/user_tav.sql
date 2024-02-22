create or replace function
marketing_analytics.user_tav(upperBound date, users array<string>) as(

  /*
    generate the total account value for a set of users at a given date
    
    simplified definition:
    - E(settled positions * market price) + cash balance
    
    marketing_analytics.user_tav(upperBound date, users array<string>)
    input: date, array of user string
    returns: an array of result structs (a table)
    
    usage:
    select * from unnest(marketing_analytics.user_tav(current_date, array(select id from production.users)))

  */
  array(
  
    with

    total_cash as(
    -- user time series
      select a.ownerId, sum(cast(cashAmount as numeric)) as totalCash
      from `production.account_ledger` led
      join `production.clearing_accounts` a on (a.id = led.accountId and a.accountOwnerType !='firm')
      where cast(timestamp(led.timestamp) as date) < upperBound
        and a.ownerId in unnest(users)
      group by a.ownerId
    ),

    positions as(
      select
        a.ownerId,
        c.ticker,
        sum(cast(stockAmount as numeric)) as position
      from `production.account_ledger` led
      join `production.clearing_accounts` a on (a.id = led.accountId and a.accountOwnerType !='firm')
      join `production.companies` c on (c.cusip = led.cusip)
      where
        cast(timestamp(led.timestamp) as date) < upperBound
        and a.ownerId in unnest(users)
      group by a.ownerId, c.ticker
    ),

    last_value_bounded as(
      -- last stock value before upper bound
      select * from(
        select *, row_number() over(partition by Symbol order by Date desc) as n
        from `marketing_analytics.daily_close_prices_dedup`
        where Date < current_date
      )
      where n = 1
    ),

    position_value as(
      select
        p.ownerId,
        sum(p.position * coalesce(d.Close, 0)) as mkt_value
      from positions p
      left join last_value_bounded d on (d.Symbol = p.ticker)
      group by p.ownerId
    )

    select
      struct(
        p.ownerId as user_id,
        upperBound as end_date,
        p.mkt_value as equity_value,
        coalesce(c.totalCash,0) as cash_value,
        p.mkt_value + coalesce(c.totalCash,0) as tav
      )
    from position_value p
    left join total_cash c on (c.ownerId = p.ownerId)
  )
);


create or replace function
marketing_analytics.cobrand_user_tav(upperBound date, users array<string>) as(

  /*
    generate the total account value for a set of users at a given date for their co-brand activity
    
    simplified definition:
    - E(settled positions * market price) + cash balance
    
    marketing_analytics.cobrand_user_tav(upperBound date, users array<string>)
    input: date, array of user string
    returns: an array of result structs (a table)
    
    usage:
    select * from unnest(marketing_analytics.cobrand_user_tav(current_date, array(select id from production.users)))

  */
  array(
  
    with
    positions as(
      select
        a.ownerId,
        p.title as program,
        p.__key__.name as program_id,
        json_value(safe.parse_json(dp.metadataJson).logoMinimal.url) as logo,
        c.ticker,
        sum(cast(stockAmount as numeric)) as position
      from `production.account_ledger` led
      join `production.clearing_accounts` a on (a.id = led.accountId and a.accountOwnerType !='firm')
      join `production.companies` c on (c.cusip = led.cusip)
      join `production.rewards` r on (r.id = led.lineage.sourceId)
      join `production.programs` p on (p.__key__.name = r.programId)
      left join `production.display-programs` dp on (dp.__key__.name = p.__key__.name)
      where
        cast(timestamp(led.timestamp) as date) < upperBound
        and a.ownerId in unnest(users)
        and p.isPublic  -- assume all co-branded entities will carry this flag as TRUE and have a cohort
        and p.cohorts is not null
      group by 1,2,3,4,5
    ),

    last_value_bounded as(
      -- last stock value before upper bound
      select * from(
        select *, row_number() over(partition by Symbol order by Date desc) as n
        from `marketing_analytics.daily_close_prices_dedup`
        where Date < upperBound
      )
      where n = 1
    ),
    
    roll as(
      select
        p.ownerId as user_id,
        p.program,
        p.program_id,
        p.logo,
        string_agg(p.ticker, ",") as tickers,
        sum(p.position * coalesce(d.Close, 0)) as mkt_value
      from positions p
      left join last_value_bounded d on (d.Symbol = p.ticker)
      group by 1,2,3,4
    )

    select
      struct(
        user_id,
        program,
        program_id,
        logo,
        tickers,
        mkt_value
      ) as position
    from roll
  )
);