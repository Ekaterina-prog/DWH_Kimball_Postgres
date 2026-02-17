-- Создание таблиц core слоя (звездная схема по Кимбалу)

-- Создание схемы core
create schema if not exists core;

-- Удаление существующих таблиц (если есть)
drop table if exists core.fact_payment;
drop table if exists core.fact_rental;

drop TABLE if exists core.dim_date;
drop table if exists core.dim_inventory;
drop table if exists core.dim_staff;

-create table core.dim_inventory (
	inventory_pk serial primary key,
	inventory_id integer not null,
	film_id integer not null,
	title varchar(255) not null,
	rental_duration int2 not null,
	rental_rate numeric(4,2) not null,
	length int2,
	rating varchar(10),
	effective_date_from timestamp not null,
	effective_date_to timestamp not null,
	is_active boolean not null
);

create table core.dim_staff (
	staff_pk serial primary key,
	staff_id integer not null,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	address varchar(50) not null,
	district varchar(20) not null,
	city_name varchar(50) not null,
	effective_date_from timestamp not null,
	effective_date_to timestamp not null,
	is_active boolean not null
);

create table core.fact_payment (
	payment_pk serial primary key,
	payment_id integer not null,
	amount numeric(7,2) not null,
	payment_date_fk integer not null references core.dim_date(date_dim_pk),
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk)
);

create table core.fact_rental (
	rental_pk serial primary key,
	rental_id integer not null,
	inventory_fk integer not null references core.dim_inventory(inventory_pk),
	staff_fk integer not null references core.dim_staff(staff_pk),
	rental_date_fk integer not null references core.dim_date(date_dim_pk),
	return_date_fk integer references core.dim_date(date_dim_pk),
	effective_date_from timestamp not null,
	effective_date_to timestamp not null,
	is_active boolean not null
);

