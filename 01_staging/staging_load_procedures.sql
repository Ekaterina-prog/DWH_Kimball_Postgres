-- Процедуры загрузки данных из источника в staging слой
-- Реализована полная перезапись данных (full reload)

create or replace function staging.get_last_update_table(table_name varchar) returns timestamp
as $$
	begin
		return coalesce( 
			(
				select
					max(update_dt)
				from
					staging.last_update lu
				where 
					lu.table_name = get_last_update_table.table_name
			),
			'1900-01-01'::date	
		);
	end;
$$ language plpgsql;

create or replace procedure staging.set_table_load_time(table_name varchar, current_update_dt timestamp default now())
as $$
	begin
		insert into staging.last_update
		(
			table_name, 
			update_dt
		)
		values
		(
			table_name, 
			current_update_dt
		);
	end;
$$ language plpgsql;

create or replace procedure staging.film_load(current_update_dt timestamp)
 as $$
	begin
		delete from staging.film;

		insert
		into
		staging.film
			(film_id,
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
			fulltext)
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
			film_src.film;

		call staging.set_table_load_time('staging.film', current_update_dt);

	end;
$$ language plpgsql;

create or replace procedure staging.inventory_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = staging.get_last_update_table('staging.inventory');
			
		delete from staging.inventory;

		insert into staging.inventory
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
			film_src.inventory i
		where 
			i.last_update >= last_update_dt
			or i.deleted >= last_update_dt;
		
		call staging.set_table_load_time('staging.inventory', current_update_dt);
		
	end;
$$ language plpgsql;

create or replace procedure staging.rental_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = staging.get_last_update_table('staging.rental');
		delete from staging.rental;

		insert into staging.rental
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
			film_src.rental
		where 
			deleted >= last_update_dt
			or last_update >= last_update_dt;

		call staging.set_table_load_time('staging.rental', current_update_dt);

	end;
$$ language plpgsql;

create or replace procedure staging.payment_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = staging.get_last_update_table('staging.payment');
		
		delete from staging.payment;

		insert into staging.payment
		(
			payment_id, 
			customer_id, 
			staff_id, 
			rental_id, 
			inventory_id,
			amount, 
			payment_date,
			last_update,
			deleted
		)
		select
			p.payment_id, 
			p.customer_id, 
			p.staff_id, 
			p.rental_id, 
			r.inventory_id,
			p.amount, 
			p.payment_date,
			p.last_update,
			p.deleted
		from
			film_src.payment p
			join film_src.rental r using (rental_id)
		where
			p.deleted >= last_update_dt
			or p.last_update >= last_update_dt
			or r.last_update >= last_update_dt;
		
		call staging.set_table_load_time('staging.payment', current_update_dt);
	end;
$$ language plpgsql;

create or replace procedure staging.staff_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin 
		last_update_dt = staging.get_last_update_table('staging.staff');
	
		delete from staging.staff;
	
		insert into staging.staff
		(
			staff_id,
			first_name,
			last_name,
			store_id,
			deleted,
			last_update
		)
		select
			staff_id,
			first_name,
			last_name,
			store_id,
			deleted,
			last_update
		from
			film_src.staff s
		where 
			s.last_update >= last_update_dt
			or s.deleted >= last_update_dt;

		call staging.set_table_load_time('staging.staff', current_update_dt);
		
	end;
$$ language plpgsql;


create or replace procedure staging.address_load()
as $$
	begin 
		delete from staging.address;
	
		insert into staging.address
		(
			address_id,
			address,
			district,
			city_id
		)
		select
			address_id,
			address,
			district,
			city_id
		from 
			film_src.address;
	end;
$$ language plpgsql;

create or replace procedure staging.city_load()
as $$
	begin 
		delete from staging.city;
	
		insert into staging.city
		(
			city_id,
			city
		)
		select
			city_id,
			city
		from
			film_src.city;

	end;
$$ language plpgsql;

create or replace procedure staging.store_load()
as $$
	begin 
		delete from staging.store;
	
		insert into staging.store
		(
			store_id,
			address_id
		)
		select
			store_id,
			address_id
		from
			film_src.store;

	end;
$$ language plpgsql;
