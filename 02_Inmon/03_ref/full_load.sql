-- полная загрузка данных

create or replace procedure full_load()
as $$
	declare
		current_update_dt timestamp = now();
	begin
		call staging.film_load(current_update_dt);
		call staging.inventory_load(current_update_dt);
		call staging.rental_load(current_update_dt);
		call staging.address_load(current_update_dt);
		call staging.city_load(current_update_dt);
		call staging.staff_load(current_update_dt);
		call staging.store_load(current_update_dt);
		call staging.payment_load(current_update_dt);
		
		call ods.film_load();
		call ods.inventory_load();
		call ods.rental_load();
		call ods.address_load();
		call ods.city_load();
		call ods.staff_load();
		call ods.store_load();
		call ods.payment_load();
		
		call film_id_sync();
		call inventory_id_sync();
		call rental_id_sync();
		call address_id_sync();
		call city_id_sync();
		call staff_id_sync();
		call store_id_sync();
		call payment_id_sync();
	end;
$$ language plpgsql;

call full_load();