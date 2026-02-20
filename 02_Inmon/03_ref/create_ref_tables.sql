--создание таблиц ref слоя

create schema ref;

drop table if exists ref.film;
create table ref.film (
	film_sk serial not null,
	film_nk int not null
);

drop table if exists ref.inventory;
create table ref.inventory (
	inventory_sk serial not null,
	inventory_nk int not null
);

drop table if exists ref.rental;
create table ref.rental (
	rental_sk serial not null,
	rental_nk int not null
);

drop table if exists ref.address;
create table ref.address (
	address_sk serial not null,
	address_nk int not null
);

drop table if exists ref.city;
create table ref.city (
	city_sk serial not null,
	city_nk int not null
);

drop table if exists ref.staff;
create table ref.staff (
	staff_sk serial not null,
	staff_nk int not null
);

drop table if exists ref.store;
create table ref.store (
	store_sk serial not null,
	store_nk int not null
);

drop table if exists ref.payment;
create table ref.payment (
	payment_sk serial not null,
	payment_nk int not null
);