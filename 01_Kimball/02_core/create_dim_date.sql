drop TABLE if exists core.dim_date;

create table core.dim_date
(
  date_dim_pk INT primary key,
  date_actual DATE not null,
  epoch BIGINT not null,
  day_suffix VARCHAR(4) not null,
  day_name VARCHAR(11) not null,
  day_of_week INT not null,
  day_of_month INT not null,
  day_of_quarter INT not null,
  day_of_year INT not null,
  week_of_month INT not null,
  week_of_year INT not null,
  week_of_year_iso CHAR(10) not null,
  month_actual INT not null,
  month_name VARCHAR(9) not null,
  month_name_abbreviated CHAR(3) not null,
  quarter_actual INT not null,
  quarter_name VARCHAR(9) not null,
  year_actual INT not null,
  first_day_of_week DATE not null,
  last_day_of_week DATE not null,
  first_day_of_month DATE not null,
  last_day_of_month DATE not null,
  first_day_of_quarter DATE not null,
  last_day_of_quarter DATE not null,
  first_day_of_year DATE not null,
  last_day_of_year DATE not null,
  mmyyyy CHAR(6) not null,
  mmddyyyy CHAR(10) not null,
  weekend_indr BOOLEAN not null
);

create index dim_date_date_actual_idx
  on
core.dim_date(date_actual);