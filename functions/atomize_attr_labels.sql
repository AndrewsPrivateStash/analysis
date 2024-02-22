create or replace function
functions.atomize_attr_labels(a string) as(
  /*
    atomize the [external partner] category labels and Bumped category names
    remove filler conjuctions
    input: production.merchants.labels.[external partner]CategoryName or production.brand_categories.name
    output: array of label atoms (tokens)
  */
  array(
  
    with
    -- combine atoms into one that are intrinsically linked
    combine_filters as(
      select
        case
          when regexp_contains(lower(trim(a)), r"ride\s+sharing") then "RideSharing"
          when regexp_contains(lower(trim(a)), r"club\s+warehouse") then "ClubWarehouse"
          when regexp_contains(lower(trim(a)), r"personal\s+care") then "PersonalCare"
          when regexp_contains(lower(trim(a)), r"meal\s+kits") then "MealKits"
          when regexp_contains(lower(trim(a)), r"ice\s+cream") then "IceCream"
          when regexp_contains(lower(trim(a)), r"sporting\s+goods") then "SportingGoods"
          when regexp_contains(lower(trim(a)), r"home\s+improvement") then "HomeImprovement"
          else trim(a)
        end as in_str    
    )
  
    select distinct tokens
    from unnest(
          split(
              regexp_replace(
                  regexp_replace(
                    regexp_replace(( select in_str from combine_filters ), r"[,|&]", ' '),
                    r"[^a-zA-Z0-9\-_ ]+", ''
                  ),
                  r" +", ' '    -- replace multiple spaces with a single space                   
              ), ' '            -- split on spaces     
          )
        ) as tokens
    where
      -- remove list
      lower(tokens) not in("and", "or", "a", "is", "of", "stores", "eats", "services", "accommodations")
      and not regexp_contains(tokens, r"\W")
  )
);