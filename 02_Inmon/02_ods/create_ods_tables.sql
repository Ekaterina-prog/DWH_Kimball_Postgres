--создание таблиц ods слоя

create schema ods;

drop table if exists ods.film;
CREATE TABLE ods.film (
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

drop table if exists ods.inventory;
CREATE TABLE ods.inventory (
	inventory_id int NOT NULL,
	film_id int2 NOT NULL,
	store_id int2 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

drop table if exists ods.rental;
CREATE TABLE ods.rental (
	rental_id int NOT NULL,
	rental_date timestamp NOT NULL,
	inventory_id int4 NOT NULL,
	customer_id int2 NOT NULL,
	return_date timestamp NULL,
	staff_id int2 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);

drop table if exists ods.address;
CREATE TABLE ods.address (
	address_id int NOT NULL,
	address varchar(50) NOT NULL,
	address2 varchar(50) NULL,
	district varchar(20) NOT NULL,
	city_id int2 NOT NULL,
	postal_code varchar(10) NULL,
	phone varchar(20) NOT NULL,
	last_update timestamp NOT NULL
);

drop table if exists ods.city;
CREATE TABLE ods.city (
	city_id int NOT NULL,
	city varchar(50) NOT NULL,
	country_id int2 NOT NULL,
	last_update timestamp NOT NULL
);

drop table if exists ods.staff;
CREATE TABLE ods.staff (
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

drop table if exists ods.store;
CREATE TABLE ods.store (
	store_id int NOT NULL,
	manager_staff_id int2 NOT NULL,
	address_id int2 NOT NULL,
	last_update timestamp NOT NULL
);

drop table if exists ods.payment;
CREATE TABLE ods.payment (
	payment_id int NOT NULL,
	customer_id int2 NOT NULL,
	staff_id int2 NOT NULL,
	rental_id int4 NOT NULL,
	amount numeric(5,2) NOT NULL,
	payment_date timestamp NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp NULL
);