/*
  table to capture exceptional merchants that should be excluded from some types of activity
  manually insert records as needed
*/
create or replace table `rec_engine.global_mid_filter` (
  mid string not null,
  mid_name string,
  direct bool options(description="can the mid be used for direct recommendations"),
  atom bool options(description="can the mid be used for atom recommendations"),
  geo bool options(description="can the mid be used for geo recommendations"),
  email bool options(description="can the mid be used for emails"),
  missed bool options(description="can the mid be used for missed offers results"),
  first_use bool options(description="should the mid only be presented if no prior activity exists for the brand"),
)
options(
  description="list of exception merchants to be excluded from at least one of the four current use-cases"
);

-- additions

-- Costco - Memberships
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  true,
  true,
  true,
  false,
  true
from `production.merchants`
where __key__.name = "9aeb5366f4594599b0150e2474bc177e"
;

-- GrubHub - First Use Offer
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  true,
  true,
  false,
  false,
  true
from `production.merchants`
where __key__.name = "3977df95718044f0878fe914f5abccd5"
;

-- Macy's (tiers)
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  true,
  false,
  false
from `production.merchants`
where __key__.name = "144fefa05296483589eb1e019ab8a8e8"
;

--  Sam's Club (tiers)
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  true,
  false,
  false
from `production.merchants`
where __key__.name = "b2459a34e1194750a6eb689bea2dfbc7"
;

--  Cost Plus (tiers)
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  true,
  false,
  false
from `production.merchants`
where __key__.name = "9e07a033035144e3b4e2fc9482c2ed45"
;

--  Firstleaf (tiers)
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  true,
  false,
  false
from `production.merchants`
where __key__.name = "39c88a3c683241288cf674b171ec793b"
;

--  Brooks Brothers (tiers)
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  true,
  false,
  false
from `production.merchants`
where __key__.name = "0b85d68fd7104021a5dfad1a2f79b950"
;

--  CVS.com
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  false,
  false,
  false
from `production.merchants`
where __key__.name = "6d5e1c32de2546ae91defb4bd7f6ebdc"
;

--  CVS Photo
insert into `rec_engine.global_mid_filter` (mid, mid_name, direct, atom, geo, email, missed, first_use)
select
  __key__.name as mid,
  title as mid_name,
  false,
  false,
  false,
  false,
  false,
  false
from `production.merchants`
where __key__.name = "81d23bb6cd004f03a61147313d24e74d"
;