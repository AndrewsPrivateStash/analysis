/*
  [EXTERNAL PARTNER]  **masked partner name
  functions to pull differential txns and user links
    
*/

/*
  DB process for [external partner]:
  
  1) call bumped-analytics-aw5325.[external partner].make_files();
  
    - this builds two files made up of differential records not contained in the associated "sent" tables
      > bumped-analytics-aw5325.[external partner].transactions_send
      > bumped-analytics-aw5325.[external partner].user_links_send

    It then writes the contents of each of the send files to the sent tables { transactions_sent, user_links_sent }
    
    The strings are cleaned of any EOL (\r,\n) characters in both send and sent tables.
    
  2) export, dump, query, or otherwise extract the contents of the two send tables for Mimic processing

*/



/* make transaction_sent table
create or replace table `[external partner].transactions_sent` as
select current_timestamp() as stamp, *
from `[external partner].transactions`
limit 1;
*/

/*
delete from `[external partner].transactions_sent` where true;
*/

/* make user_state table
create or replace table `[external partner].user_links_sent` as
select
  1 as version, 
  current_timestamp() as stamp,
  *
from `[external partner].user_links`
limit 1;
*/

/*
delete from `[external partner].user_links_sent` where true;
*/


create or replace function
[external partner].get_diff_transactions() as(
  /*
    get the differential set of transactions for MIMIC processing
    using [external partner].transactions_sent as the filtering table
    all transaction not in filtering table are returned as array of structs (table)
  */
array(
  select struct(
    current_timestamp() as stamp,
    transaction_id,
    card_type,
    account_id,
    card_id,
    date,
    amount,
    transaction_sign,
    description,
    transaction_type,
    transaction_category_tags,
    merchant_city,
    merchant_state,
    merchant_zip_code,
    merchant_country,
    date_transaction_receieved,
    tagged_brand
  )
  from(
    select * from `[external partner].transactions`
    where transaction_id not in( select transaction_id from `[external partner].transactions_sent` )
  )
))
;


create or replace function
[external partner].get_diff_user_links() as(
/*
  get the differential user card links (new records) as well as any changed records
  record insert stamp and version number
  multiple versions allowed in table to keep state history, when sending use highest version
*/
array(

  with
  new_records as(
    select *
    from `[external partner].user_links`
    except distinct
    select * except(version, stamp) from `[external partner].user_links_sent`
  ),

  update_records as(
    select uls.version + 1 as version, ul.*
    from `[external partner].user_links` ul
    join (
      select *
      from `[external partner].user_links_sent`
      where true
      qualify row_number() over(partition by account_id, card_id order by version desc) = 1
    ) uls on(uls.account_id = ul.account_id and uls.card_id = ul.card_id)
    where
         ul.customer_id != uls.customer_id 
      or ul.link_type != uls.link_type 
      or ul.link_subtype != uls.link_subtype 
      or ul.account_id != uls.account_id 
      or ul.card_id != uls.card_id 
      or ul.zip != uls.zip 
      or ul.dob != uls.dob 
      or ul.firstName != uls.firstName 
      or ul.middleName != uls.middleName 
      or ul.lastName != uls.lastName 
      --or ul.address_line_1 != uls.address_line_1 
      --or ul.address_line_2 != uls.address_line_2
      --or ul.latitude != uls.latitude
      --or ul.longitude != uls.longitude
      or ul.city != uls.city 
      or ul.state_code != uls.state_code 
      or ul.gender != uls.gender
  ),

  out_data as(
    select
      current_timestamp() as stamp,
      1 as version,
      *
    from new_records

    union all

    select
      current_timestamp() as stamp,
      *
    from update_records
  )
  
  select
    struct(
      version,
      stamp,
      customer_id,
      link_type,
      link_subtype,
      account_id,
      card_id,
      zip,
      dob,
      firstName,
      middleName,
      lastName,
      --address_line_1,
      --address_line_2,
      --latitude,
      --longitude,
      city,
      state_code,
      gender
    )
  from out_data 
))
;


create or replace procedure [external partner].make_files()
/*
  make the send files from differential unsent (or updated) records

*/

begin

  -- build diffs first using state of sent file, save results to send files
  create or replace table `[external partner].transactions_send` as
  select
    stamp,
    transaction_id,
    card_type,
    account_id,
    card_id,
    date,
    amount,
    transaction_sign,
    [external partner].clean_string(description) as description,
    transaction_type,
    transaction_category_tags,
    [external partner].clean_string(merchant_city) as merchant_city,
    [external partner].clean_string(merchant_state) as merchant_state,
    [external partner].clean_string(merchant_zip_code) as merchant_zip_code,
    [external partner].clean_string(merchant_country) as merchant_country,
    date_transaction_receieved,
    tagged_brand
  from unnest( [external partner].get_diff_transactions() )
  ;

  create or replace table `[external partner].user_links_send` as
  select
    version,
    stamp,
    customer_id,
    link_type,
    link_subtype,
    account_id,
    card_id,
    zip,
    dob,
    [external partner].clean_string(firstName) as firstName,
    [external partner].clean_string(middleName) as middleName,
    [external partner].clean_string(lastName) as lastName,
    [external partner].clean_string(city) as city,
    state_code,
    gender
  from unnest( [external partner].get_diff_user_links() )
  ;

end
;

create or replace procedure [external partner].log_sent()
/*
  insert into transactions_sent and user_links_sent from send files

*/

begin

  -- write diffs to sent files
  insert into `[external partner].transactions_sent`
  select * from `[external partner].transactions_send`
  ;

  insert into `[external partner].user_links_sent`
  select * from `[external partner].user_links_send`
  ;
  
  --clean send files
  truncate table [external partner].transactions_send;
  truncate table [external partner].user_links_send;

end
;




/* create a sample deficit
delete `[external partner].user_links_sent` as f
where exists(
  with
    samp as(
      select *
      from `[external partner].user_links_sent`
      order by rand()
      limit 50
    )
  select * from samp s
  where s.account_id = f.account_id and s.card_id = f.card_id
)
*/

-- remove EOL chars from fields
create or replace function
[external partner].clean_string(in_str string) as(
  regexp_replace(
    regexp_replace(in_str, r"[\n\r]", " "),
    r" +",
    " "
  )
)
;
  

-- add an excluded user to [external partner] data
insert into `[external partner].excluded_users` (user_id)
values("f3591a08-6ccc-41e4-9cf7-0ef39b8bc82d")
