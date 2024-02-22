create or replace function
functions.tokenize(a string) as(
  /*
    cleans string and splits into sorted word tokens
    input: uncleaned string
    returns: sorted array of token strings
  */
    array(select x from unnest(
        array(
        select tokens
        from
            unnest(
                split(
                    regexp_replace(
                        regexp_replace(
                            -- remove leading and trailing whitespace, then swap uncommon seperators with spaces
                            lower(regexp_replace(trim(a), r"[,-/_\.]", ' ')),
                            r"[^a-z0-9 ]+", ''   -- remove anything that isn't a letter, number, space
                        ),
                        r" +", ' '    -- replace multiple spaces with a single space                   
                    ), ' '            -- split on spaces     
                )
            ) tokens
        )) as x
        order by x
    )
);
