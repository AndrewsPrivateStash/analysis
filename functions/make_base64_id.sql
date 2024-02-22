create or replace function
functions.make_base64_id(a string, b string) as(
/*
    functions.make_base64_id(a string, b string)
    input: prefix, value
    returns: base64 encodeed string
    make_base64_id("User", "fd8fdfad-fbe0-41c3-8bea-62b0c0a56e0b") -> VXNlcjpmZDhmZGZhZC1mYmUwLTQxYzMtOGJlYS02MmIwYzBhNTZlMGI=
*/
  (
    select to_base64(cast(initcap(a) || ':' || b as bytes))
  )

)
;