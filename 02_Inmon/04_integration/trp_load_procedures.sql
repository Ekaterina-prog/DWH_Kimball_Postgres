-- создание ods процедур

create or replace procedure ods.film_load()
 as $$
 	begin
		delete from ods.film;
	
		insert into	ods.film
		(
			film_id,
			title,
			description,
			release_year,
			language_id,
			rental_duration,
			rental_rate,
			length,
			replacement_cost,
			rating,
			last_update,
			special_features,
			fulltext
		)
		select 
			film_id,
			title,
			description,
			release_year,
			language_id,
			rental_duration,
			rental_rate,
			length,
			replacement_cost,
			rating,
			last_update,
			special_features,
			fulltext
		from
			staging.film;
	end;
$$ language plpgsql;

create or replace procedure ods.inventory_load()
as $$
	begin
		delete from ods.inventory;
	
		insert into ods.inventory
		(
			inventory_id, 
			film_id, 
			store_id,
			last_update,
			deleted 
		)
		select 
			inventory_id, 
			film_id, 
			store_id,
			last_update,
			deleted
		from
			staging.inventory i;
	end;
$$ language plpgsql;

create or replace procedure ods.rental_load()
as $$
	begin
		delete from ods.rental odr 
		where odr.rental_id in (
			select 
				sr.rental_id
			from 
				staging.rental sr
		);
	
		insert into ods.rental
		(
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id,
			last_update,
			deleted
		)
		select 
			rental_id, 
			rental_date, 
			inventory_id, 
			customer_id, 
			return_date, 
			staff_id,
			last_update,
			deleted
		from
			staging.rental;
	end;

$$ language plpgsql;

create or replace procedure ods.address_load()
 as $$
 	begin
		delete from ods.address;
	
		insert into	ods.address
		(
			address_id,
			address,
			address2,
			district,
			city_id,
			postal_code,
			phone,
			last_update
		)
		select 
			address_id,
			address,
			address2,
			district,
			city_id,
			postal_code,
			phone,
			last_update
		from
			staging.address;
	end;
$$ language plpgsql;

create or replace procedure ods.city_load()
 as $$
 	begin
		delete from ods.city;
	
		insert into	ods.city
		(
			city_id,
			city,
			country_id,
			last_update
		)
		select 
			city_id,
			city,
			country_id,
			last_update
		from
			staging.city;
	end;
$$ language plpgsql;

create or replace procedure ods.staff_load()
 as $$
 	begin
		delete from ods.staff;
	
		insert into	ods.staff
		(
			staff_id,
			first_name,
			last_name,
			address_id,
			email,
			store_id,
			active,
			username,
			last_update,
			picture,
			deleted
		)
		select 
			staff_id,
			first_name,
			last_name,
			address_id,
			email,
			store_id,
			active,
			username,
			last_update,
			picture,
			deleted
		from
			staging.staff;
	end;
$$ language plpgsql;

create or replace procedure ods.store_load()
 as $$
 	begin
		delete from ods.store;
	
		insert into	ods.store
		(
			store_id,
			manager_staff_id,
			address_id,
			last_update
		)
		select 
			store_id,
			manager_staff_id,
			address_id,
			last_update
		from
			staging.store;
	end;
$$ language plpgsql;

create or replace procedure ods.payment_load()
as $$
	begin
		delete from ods.payment odp 
		where odp.payment_id in (
			select 
				sp.payment_id
			from 
				staging.payment sp
		);
		
		insert into ods.payment
		(
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
		)
		select 
			payment_id,
			customer_id,
			staff_id,
			rental_id,
			amount,
			payment_date,
			last_update,
			deleted
		from
			staging.payment;
	end;
$$ language plpgsql;



