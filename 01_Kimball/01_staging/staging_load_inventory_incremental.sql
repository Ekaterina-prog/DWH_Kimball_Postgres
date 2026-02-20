-- Инкрементальная загрузка staging.inventory
-- Отслеживание изменений через staging.last_update

create or replace procedure staging.inventory_load()
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = coalesce( 
			(
				select
					max(update_dt)
				from
					staging.last_update
				where 
					table_name = 'staging.inventory'
			),
			'1900-01-01'::date	
		);
		
		delete from staging.inventory;

		insert into staging.inventory
		(
			inventory_id, 
			film_id, 
			store_id,
			deleted
			
		)
		select 
			inventory_id, 
			film_id, 
			store_id,
			deleted
		from
			film_src.inventory i
		where 
			i.last_update >= last_update_dt
			or i.deleted >= last_update_dt;
		
		INSERT INTO staging.last_update
		(
			table_name, 
			update_dt
		)
		VALUES(
			'staging.inventory', 
			now()
		);

	end;
$$ language plpgsql;
