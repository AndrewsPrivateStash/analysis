/*
    transaction detail with linkId as foreign key
    **masked partner name
*/

create or replace view [external partner].transactions as
with
plaid_txns as(
  select
    tx.transaction_id,  -- primary key in resulting view
    tx.pending_transaction_id,
    accounts.subtype as card_type,
    accounts.id as account_id,
    links.id as card_id,
    parse_date("%F", tx.date) as date,
    cast(coalesce(tx.amount.float, tx.amount.integer) as numeric) as amount,
    if( coalesce(tx.amount.float, tx.amount.integer) >= 0, '+', '-' ) as transaction_sign,
    
    tx.original_description as description,
    tx.transaction_type,
    array_to_string(tx.category, ',') as transaction_category_tags,
    
    tx.location.city as merchant_city,
    tx.location.state as merchant_state,
    coalesce(tx.location.postal_code, tx.location.zip) as merchant_zip_code,
    tx.location.country as merchant_country, -- only US activity currently
    timestamp(t.createdAt) as date_transaction_receieved,
    b.name as tagged_brand

  from `bumped-analytics-aw5325.production.plaid_tx` tx
  join `bumped-analytics-aw5325.production.plaid_accounts` accounts on (tx.account_id = accounts.id)
  join `bumped-analytics-aw5325.production.plaid_links` links on (accounts.linkId = links.id)
  left join `production.transactions` t on(t.plaidId = coalesce(tx.pending_transaction_id, tx.transaction_id))
  left join `production.brands` b on(b.id = t.brandId)
  where pending = false
    and links.userId not in( select user_id from [external partner].excluded_users ) 
)

select * except(pending_transaction_id)
from plaid_txns p
;



/*
    card-link / customer detail with linkId as primary key

*/

create or replace view [external partner].user_links as
with
user_geo as(
  select
    t.userId,
    t.streetAddress,
    t.unit,
    t.city,
    t.postalCode as zipcode,
    coalesce(za.state_code, t.state) as state_code,
    za.latitude,
    za.longitude
  from (
    select *, row_number() over(partition by userId) as n
    from `bumped-analytics-aw5325.production.user_addresses`
    where type = 'HOME'
  ) t
  left join `bigquery-public-data.utility_us.zipcode_area` za on(za.zipcode = t.postalCode)
  where n = 1 
),

user_gender as(
  -- estimated value based on cesus first-name mapping (for directional use only)
  -- indeterminate names are mapped to "Unknonwn"
  select
    u.id as user_id,
    coalesce(gender, 'NA') as gender
  from production.users u
  left join `analytics_views.census_names_vw` c on(
    c.year = extract(year from parse_date('%F', u.dateOfBirth))
    and c.name = coalesce(u.preferredName, u.firstName)
  )
),

users as(
  select distinct
    links.userId as customer_id,
    accounts.type as link_type,
    accounts.subtype as link_subtype,
    accounts.id as account_id,
    links.id as card_id,
    geo.zipcode as zip,
    parse_date('%F', u.dateOfBirth) as dob,
    u.firstName,
    u.middleName,
    u.lastName,
    --geo.streetAddress as address_line_1,
    --geo.unit as address_line_2,
    --geo.latitude,
    --geo.longitude,
    geo.city,
    geo.state_code,
    gen.gender -- {M, F, NA}
    
  from `bumped-analytics-aw5325.production.plaid_tx` tx
  join `bumped-analytics-aw5325.production.plaid_accounts` accounts on (tx.account_id = accounts.id)
  join `bumped-analytics-aw5325.production.plaid_links` links on (accounts.linkId = links.id)  
  join `production.users` u on(u.id = links.userId)
  left join user_geo geo on (geo.userId = links.userId)
  left join user_gender gen on (gen.user_id = links.userId)
  where links.userId not in( select user_id from [external partner].excluded_users ) 
)

select *
from users
;
