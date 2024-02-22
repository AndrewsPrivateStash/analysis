/* google's way */
select
  code_points_to_string([code_point]) as char,
  code_point,
  count(*) as char_cnt
from `[EXTERNAL PARTNER].transactions_sent`,
  unnest(to_code_points(description)) as code_point
group by 1,2
order by 3 desc;


/* iterate 
select
  char,
  unicode(char) as unicode,
  sum(char_cnt) as tot_char_cnt
from(
  select
    d,
    char,
    count(*) as char_cnt
  from(
    select
      description as d,
      split(description, '') as char_arr
    --from unnest(["hmmm a string","and another string", "yet another string! \t\n\r"]) as d
    from `[EXTERNAL PARTNER].transactions_sent`
  ) t
  cross join unnest(char_arr) as char
  group by 1,2
)
group by 1,2
order by 3 desc
*/


/* batch roll
select
  char,
  unicode(char) as unicode,
  freq
from(
  select
    char,
    count(*) as freq
  from unnest((
      select
        array_concat_agg(
          split(description,'')
        ) as chars
      from unnest(["hmmm a string","and another string", "yet another string! \t\n\r"]) as description
  )) as char
  group by 1
)
order by freq desc
*/
