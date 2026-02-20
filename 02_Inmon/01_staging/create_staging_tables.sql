-- Создание таблиц staging слоя по Инмону
-- Таблицы создаются на основе структуры таблиц источника

create schema staging;

drop table if exists staging.last_update;
create table staging.last_update (
	table_name varchar(50) not null,
	update_dt timestamp not null
);

drop table if exists staging.film;
CREATE TABLE staging.film (
	film_id int NOT NULL,
	title varchar(255) NOT NULL,
	description text NULL,
	release_year year NULL,
	language_id int2 NOT NULL,
	rental_duration int2 NOT NULL,
	rental_rate numeric(4,2) NOT NULL,
	length int2 NULL,
	replacement_cost numeric(5,2) NOT NULL,
	rating mpaa_rating NULL,
	last_update timestamp NOT NULL,
	special_features _text NULL,
	fulltext tsvector NOT NULL
);

drop table if exists staging.inventory;
CREATE TABLE staging.inventory (
	inventory_id int NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

drop table if exists staging.rental;
CREATE TABLE staging.rental (
	rental_id int NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

drop table if exists staging.address;
CREATE TABLE staging.address (
	address_id int NOT NULL,
	address varchar(50) NOT NULL,
	address2 varchar(50) NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL,
	postal_code varchar(10) NULL,
	phone varchar(20) NOT NULL,
	last_update timestamp NOT NULL
);

drop table if exists staging.city;
CREATE TABLE staging.city (
	city_id int NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	last_update timestamp NOT NULL
);

drop table if exists staging.staff;
CREATE TABLE staging.staff (
	staff_id int NOT NULL,
	first_name varchar(45) NOT NULL,
	last_name varchar(45) NOT NULL,
	address_id int2 NOT NULL,
	email varchar(50) NULL,
	store_id int2 NOT NULL,
	active bool NOT NULL,
	username varchar(16) NOT NULL,
	"password" varchar(40) NULL,
	last_update timestamp NOT NULL,
	picture bytea NULL,
	deleted timestamp NULL
);

drop table if exists staging.store;
CREATE TABLE staging.store (
	store_id int NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	last_update timestamp NOT NULL
);

drop table if exists staging.payment;
CREATE TABLE staging.payment (
	payment_id int NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5,2) NOT NULL,
	payment_date timestamp NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);