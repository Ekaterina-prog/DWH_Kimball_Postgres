-- Общая процедура полной загрузки
-- Последовательно загружается staging и core слой

create or replace procedure full_load()
as $$
	declare 
			current_update_dt timestamp = now();
	begin
		call staging.film_load(current_update_dt);
		call staging.inventory_load(current_update_dt);
		call staging.rental_load(current_update_dt);
		call staging.payment_load();
		call staging.staff_load(current_update_dt);
		call staging.address_load();
		call staging.city_load();
		call staging.store_load();
		
		
		call core.load_inventory();
		call core.load_staff();
		call core.load_payment();
		call core.load_rental();
	
		call report.sales_date_calc();
		call report.sales_film_calc();
	end;
$$ language plpgsql;

call full_load();