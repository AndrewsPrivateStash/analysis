create or replace function
functions.user_geo(a array<string>) as(
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
      from `bumped-analytics-aw5325.production.user_addresses`
      where userId in unnest( a )
        and type = 'HOME'
    ) t
    left join `bigquery-public-data.utility_us.zipcode_area` za on(za.zipcode = t.postalCode)
    left join `bumped-analytics-aw5325.analytics_views.state_dim` sd on(sd.state_code = coalesce(za.state_code, t.state))
    where n = 1 -- ensure user dim
 )
);