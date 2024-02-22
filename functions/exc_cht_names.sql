create or replace function
functions.exc_cht_names(a array<string>) as(
  /*
    get exluded cohort names to clean up otherwise verbose code
    input: array of attribute strings
    output: array of cohort names
  */
  
  array(
    select cohort
    from `analytics_views.exc_chts`
    where attr in unnest(a)
      or array_length(a) = 0
      or '' in unnest(a)
  )

);