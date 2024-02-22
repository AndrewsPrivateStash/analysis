create or replace function
functions.affinity_score( tc Int64, ta Float64, sp Int64, uc Int64 ) as(
  /*
    produce an affnity score based on transaction counts and amounts
    input: int transaction count, float transaction amount, int span in days, and user count
    output: float affinity score
  */
  
  (
    -- monthly txn velocity scaled by normed spend
    case
      when sp < 7 then 0
      when tc < 10 then 0
      when ta < 10 then 0
      when uc < 10 then 0
      else safe_divide(tc, sp) * 365 / 12 * log(ta, 10) * log(uc, 10)
    end
  )

);

create or replace function
functions.affinity_score_user( tc Int64, ta Float64, sp Int64, lag Int64 ) as(
  /*
    produce an affnity score based on transaction counts and amounts
    input: int transaction count, float transaction amount, int span in days, int lag in days since last txn
    output: float affinity score

    inverse lag factor:
    Exp[-0.0124603 * lag]
    { {0,1}, {90,0.33}, {180,0.11}, {360,0.01} }
    lag is days since last txn
  */
  
  (
    -- monthly txn velocity scaled by normed spend
    case
      when sp < 7 then 0
      when tc < 5 then 0
      when ta < 10 then 0
      when lag > 365 then 0
      else safe_divide(tc, sp) * 365 / 12 * log(ta, 10) * exp(-0.0124603 * lag)
    end
  )

);