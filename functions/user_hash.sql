create or replace function functions.user_mask(in_str string) as (

(select
  substr(hex_string, 1, 8) || '-' ||
  substr(hex_string, 9, 4) || '-' ||
  substr(hex_string, 13, 4) || '-' ||
  substr(hex_string, 17, 4) || '-' ||
  substr(hex_string, 21)

from(
  select to_hex(md5(in_str)) as hex_string
)))
;

create or replace function functions.user_unmask(in_str string) as (

(
  select
    id
  from(
    select id, functions.user_mask(id) as mask_id from production.users
  )
  where mask_id = in_str

))
;


-- uniqueness proof, should be null
select
  user_id2,
  count(*) as cnt

from(
  select
    id as user_id,
    functions.user_mask(id) as user_id2
  from `production.users`
)

group by 1
having count(*) > 1
;

-- uniquness test 2
select
  user_id,
  user_id2,
  u.id as bad_join
from(
  select
    id as user_id,
    functions.user_mask(id) as user_id2
  from `production.users`
) t
join `production.users` u on(t.user_id2 = u.id)
;