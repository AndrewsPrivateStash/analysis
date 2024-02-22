create or replace function
functions.from_b64_to_string_id(a string) as(
/*
    functions.from_b64_to_string_id(a string)
    input: b64 value
    returns: array of prefix and id value
    from_b64_to_string_id("VXNlcjpmZDhmZGZhZC1mYmUwLTQxYzMtOGJlYS02MmIwYzBhNTZlMGI=") -> [User, fd8fdfad-fbe0-41c3-8bea-62b0c0a56e0b]
*/
  (
    select split(safe_convert_bytes_to_string(coalesce(from_base64(a), b"01")), ":")
  )

)
;