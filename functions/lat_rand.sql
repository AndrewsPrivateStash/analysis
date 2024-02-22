create or replace function
functions.lat_rand(a array<string>) as(
  /*
    input: an array of strings
    returns: single random element from array
  */
  (
    select a
    from unnest(a) as a
    where a is not null and a != ""
    order by rand()
    limit 1
  )
);