create or replace function
functions.array_intersection(a array<string>, b array<string>) as(
  /*
    input: two string arrays to compare
    returns: array with the common elements
  */
  array(
    select *
    from unnest(a) as a
    
    intersect distinct
    
    select *
    from unnest(b) as b
  )
);

create or replace function
functions.array_union(a array<string>, b array<string>) as(
  /*
    input: two string arrays to compare
    returns: array with union of elements (not distinct)
  */
  array(
    select *
    from unnest(a) as a
    
    union all
    
    select *
    from unnest(b) as b
  )
);

create or replace function
functions.array_union_dist(a array<string>, b array<string>) as(
  /*
    input: two string arrays to compare
    returns: array with union of distinct elements
  */
  array(
    select *
    from unnest(a) as a
    
    union distinct
    
    select *
    from unnest(b) as b
  )
);

create or replace function
functions.array_diff(a array<string>, b array<string>) as(
  /*
    input: two string arrays to compare
    returns: array with elements a - b
  */
  array(
    select *
    from unnest(a) as a
    
    except distinct
    
    select *
    from unnest(b) as b
  )
);

create or replace function
functions.array_sim_diff(a array<string>, b array<string>) as(
  /*
    symmetric difference (disjunctive union) of two sets    
    input: two string arrays to compare
    returns: array with elements (a - b) U (b - a)
  */
  array(
    (
      select *
      from unnest(a) as a

      except distinct

      select *
      from unnest(b) as b
    )
    
    union distinct
    
    (
      select *
      from unnest(b) as a

      except distinct

      select *
      from unnest(a) as b
    )
  )
);

create or replace function
functions.to_distinct(a array<string>) as(
  /*
    input: an array of strings
    returns: distinct array of strings
  */
  array(
    select distinct a
    from unnest(a) as a
  )
);


create or replace function
functions.arr_sort_asc(a array<string>) as(
  /*
    input: an array of strings
    returns: sorted (asc) array of strings
  */
  array(
    select a
    from unnest(a) as a
    order by a
  )
);