create or replace function
functions.norm_user( u string ) as(

  /*
    produce a user_id appropriate for bigquery from any user string known
    input: string
    output: raw user_id string
  */
  (
  select
    case
      when byte_length(u) = 56 then split(safe_convert_bytes_to_string(coalesce(from_base64(u), b"01")), ":")[safe_offset(1)]
      when byte_length(u) = 36 then u
      else null
    end as user_id
  )
  
);