-- Создание таблиц staging слоя
-- Таблицы создаются на основе структуры таблиц источника

create schema if not exists staging;

drop table if exists staging.last_update;
drop table if exists staging.film;
drop table if exists staging.inventory;
drop table if exists staging.rental;
drop table if exists staging.payment;
drop table if exists staging.staff;
drop table if exists staging.address;
drop table if exists staging.city;
drop table if exists staging.store;

create table staging.last_update (
	table_name varchar(50) not null,
	update_dt timestamp not null
);

create table staging.film (
    film_id int not null,
    title varchar(255) not null,
    description text null,
    release_year int2 null,
    language_id int2 not null,
    rental_duration int2 not null,
    rental_rate numeric(4, 2) not null,
    length int2 null,
    replacement_cost numeric(5, 2) not null,
    rating varchar(10) null,
    last_update timestamp not null,
    special_features _text null,
    fulltext tsvector not null
);

create table staging.inventory (
	inventory_id int4 not null,
	film_id int2 not null,
	store_id int2 not null,
	last_update timestamp not null,
	deleted timestamp null
);

create table staging.rental (
	rental_id int4 not null,
	rental_date timestamp not null,
	inventory_id int4 not null,
	customer_id int2 not null,
	return_date timestamp null,
	staff_id int2 not null,
	last_update timestamp not null,
	deleted timestamp
);

create table staging.payment (
    payment_id int4 not null,
    customer_id int2 not null,
    staff_id int4 not null,
    rental_id int4 not null,
    amount numeric(5, 2) not null,
    payment_date timestamp not null
);

create table staging.staff (
    staff_id int4 not null,
    first_name varchar(45) not null,
    last_name varchar(45) not null,
    store_id int2 not null
);

create table staging.address (
    address_id int4 not null,
    address varchar(50) not null,
    district varchar(20) not null,
    city_id int2 not null
);

create table staging.city (
    city_id int4 not null,
    city varchar(50) not null
);

create table staging.store (
    store_id integer not null,
    address_id int2 not null
);