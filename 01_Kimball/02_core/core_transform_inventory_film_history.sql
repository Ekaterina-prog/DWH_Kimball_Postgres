-- Историчность по таблице film

-- получаем время предыдущей загрузки данных в staging.film, чтобы получить изменные фильмы
film_prev_update = (
	with lag_update as (
		select
			lag(lu.update_dt) over (order by lu.update_dt) as lag_update_dt
		from 
			staging.last_update lu
		where
			lu.table_name = 'staging.film'
	)
		select max(lag_update_dt) from lag_update
);
		
-- получаем список измененных фильмов с момента предыдущей загрузки
create temporary table updated_films on commit drop as
select
	f.film_id,
	f.title,
	f.rental_duration,
	f.rental_rate,
	f.length,
	f.rating,
	f.last_update
from
	staging.film f
where 
	f.last_update >= film_prev_update;
		
-- строки в core.dim_inventory, которые нужно поменять
create temporary table dim_inventory_rows_to_update on commit drop as
select
	di.inventory_pk,
	uf.last_update
from 
	core.dim_inventory di
	join updated_films uf
		on uf.film_id = di.film_id
		and uf.last_update > di.effective_date_from
		and uf.last_update < di.effective_date_to;

-- вставляем строки с новыми значениями фильмов
insert into core.dim_inventory
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
	di.inventory_id,
	di.film_id,
	uf.title,
	uf.rental_duration,
	uf.rental_rate,
	uf.length,
	uf.rating,
	uf.last_update as effective_date_from,
	di.effective_date_to,
	di.is_active
from 
	core.dim_inventory di
	join dim_inventory_rows_to_update ru
		on ru.inventory_pk = di.inventory_pk
	join updated_films uf
		on uf.film_id = di.film_id;

-- устанавливаем дату окончания действия строк для предыдущих параметров фильмов
update core.dim_inventory di
set
	effective_date_to = ru.last_update,
	is_active = false
from 
	dim_inventory_rows_to_update ru
where 
	ru.inventory_pk = di.inventory_pk;