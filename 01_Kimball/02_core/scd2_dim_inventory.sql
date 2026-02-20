drop table if exists core.dim_inventory;

create table core.dim_inventory (
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

create or replace procedure core.load_inventory()
as $$
	begin 
		-- помечаем удаленные записи
		update core.dim_inventory i
		set
			is_active = false,
			effective_date_to = si.deleted
		from 
			staging.inventory si
		where 
			si.deleted is not null
			and i.inventory_id = si.inventory_id
			and i.is_active is true;

		-- получаем список идентификаторов новых компакт дисков
		create temporary table new_inventory_id_list on commit drop as -- удаление автоматически
		select
			i.inventory_id
		from 
			staging.inventory i
			left join core.dim_inventory di using(inventory_id)
		where 
			di.inventory_id is null;

		-- добавляем новые компакт диски в измерение dim_inventory
		insert
			into
			core.dim_inventory
		(
			inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			i.inventory_id,
			i.film_id,
			f.title,
			f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating,
			'1900-01-01'::date as effective_date_from ,
			coalesce(i.deleted, '9999-01-01'::date) as effective_date_to,
			true as is_active
		from
			staging.inventory i
			join staging.film f using(film_id)
			join new_inventory_id_list idl using(inventory_id);

		-- помечаем изменные компакт диски не активными
		update core.dim_inventory i
		set 
			is_active = false,
			effective_date_to = si.last_update
		from 
			staging.inventory si
			left join new_inventory_id_list idl using(inventory_id)
		where 
			idl.inventory_id is null
			and si.deleted is null
			and i.inventory_id = si.inventory_id
			and i.is_active is true;
		
		-- по измененым компакт дискам добавляем актуальные строки
		insert
			into
			core.dim_inventory
		(
			inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			i.inventory_id,
			i.film_id,
			f.title,
			f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating,
			i.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active 
		from
			staging.inventory i
			join staging.film f using(film_id)
			left join new_inventory_id_list idl using(inventory_id)
		where 
			idl.inventory_id is null
			and i.deleted is null;
	end;
$$ language plpgsql;