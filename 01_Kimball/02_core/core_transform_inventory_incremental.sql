-- Инкрементальная трансформация inventory в core слой
-- Обработка удалений и upsert

create or replace procedure core.load_inventory()
as $$
	begin 
		delete from core.dim_inventory i
		where i.inventory_id in (
			select 
				inv.inventory_id 
			from 
				staging.inventory inv
			where 
				inv.deleted is not null
		);
	
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
			rating
		)
		select
			i.inventory_id,
			i.film_id,
			f.title,
			f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating 
		from
			staging.inventory i
			join staging.film f using(film_id)
		where 
			i.deleted is null
		on conflict (inventory_id) do update
		set
			film_id = excluded.film_id,
			title = excluded.title,
			rental_duration = excluded.rental_duration,
		    rental_rate = excluded.rental_rate,
		    length = excluded.length,
		    rating = excluded.rating;

	end;
$$ language plpgsql;


