create or replace function
functions.levdist_ratio(a string, b string) as ( 
	/*
    https://en.wikipedia.org/wiki/Levenshtein_distance
    calcuate the ratio of unmutated chars to char count
		input: Two strings to compare
		returns: The Levenshtein similarity ratio
	*/
	safe_divide((length(a) + length(b) - fhoffa.x.levenshtein(a, b)) , (length(a) + length(b)) )
);


create or replace function
functions.levdist_token_sort_ratio(a string, b string) as( 
  /*
    use sorted tokens to calculation levdist ratio
    input: two strings to compare
    returns: the levenshtein similarity ratio from sorted tokens
  */ 
  functions.levdist_ratio(
    array_to_string(functions.tokenize(a),''),
    array_to_string(functions.tokenize(b),'')
  )
);


create or replace function
functions.levdist_token_set_ratio(a string, b string) as( 
/*
  input: two strings to compare.
  returns: the levenshtein similarity of the maximum ratio between the different token sets.
 */ 
array(
  select max(x) from
    unnest( [
      # first ratio is sorted intersection and combined a diff b
      functions.levdist_ratio(
      
        array_to_string(
          functions.array_intersection(
            functions.tokenize(a),
            functions.tokenize(b)
          ),''
        ),

        concat(
          array_to_string(
            functions.array_intersection(
              functions.tokenize(a),
              functions.tokenize(b)
            ),''
          ),
          array_to_string(
            functions.array_diff(
              functions.tokenize(a),
              functions.tokenize(b)
            ),''
          )
        )
        
      ),

      # second ratio is sorted intersection and combined b diff a
      functions.levdist_ratio(
      
        array_to_string(
          functions.array_intersection(
            functions.tokenize(a),
            functions.tokenize(b)
          ),''
        ),

        concat(
          array_to_string(
            functions.array_intersection(
              functions.tokenize(a),
              functions.tokenize(b)
            ),''
          ),
          array_to_string(
            functions.array_diff(
              functions.tokenize(b),
              functions.tokenize(a)
            ),''
          )
        )
        
      ),
    
      # third ratio is a diff b and b diff a
      functions.levdist_ratio(
        concat(
          array_to_string(
            functions.array_intersection(
              functions.tokenize(a),
              functions.tokenize(b)
            ),''
          ),
          array_to_string(
            functions.array_diff(
              functions.tokenize(a),
              functions.tokenize(b)
            ),''
          )
        ),

        concat(
          array_to_string(
            functions.array_intersection(
              functions.tokenize(a),
              functions.tokenize(b)
            ),''
          ),
          array_to_string(
            functions.array_diff(
              functions.tokenize(b),
              functions.tokenize(a)
            ),''
          )
        )
      )
    ] ) as x)[offset(0)]
);