-- Процедуры загрузки данных в core слой
-- Реализована полная перезапись (full reload)

create or replace procedure core.load_inventory()
as $$
	declare 
		film_prev_update timestamp;
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
			i.deleted is null as is_active
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

	end;
$$ language plpgsql;

create or replace procedure core.load_staff()
as $$
	begin 
		--  помечаем удаленные записи
		update core.dim_staff s
		set 
			is_active = false,
			effective_date_to = si.deleted 
		from 
			staging.staff si
		where 
			si.deleted is not null
			and s.staff_id = si.staff_id
			and s.is_active is true;
		
		-- получаем список идентификаторов новых сотрудников
		create temporary table new_staff_id_list on commit drop as 
		select
			s.staff_id 
		from
			staging.staff s
			left join core.dim_staff ds using(staff_id)
		where 
			ds.staff_id is null;

		-- добавляем новых сотрудников в измерение dim_staff
		INSERT INTO core.dim_staff
		(
			staff_id, 
			first_name, 
			last_name, 
			address, 
			district, 
			city_name,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			s.staff_id,
			s.first_name,
			s.last_name,
			ad.address,
			ad.district,
			ct.city as city_name,
			'1900-01-01'::date as effective_date_from,
			coalesce(s.deleted, '9999-01-01'::date) as effective_date_to,
			s.deleted is null as is_active 
		from
			new_staff_id_list ns
			join staging.staff s on s.staff_id = ns.staff_id
			join staging.store st using(store_id)
			join staging.address ad on ad.address_id = st.address_id 
			join staging.city ct using (city_id);
		
		-- помечаем измененных сотрудников не активными
		update core.dim_staff s
		set
			is_active = false,
			effective_date_to = ss.last_update 
		from 
			staging.staff ss
			left join new_staff_id_list idl using(staff_id)
		where 
			idl.staff_id is null
			and ss.deleted is null
			and s.staff_id = ss.staff_id 
			and s.is_active is true;
			
		-- по измененным сотрудникам добавляем актуальные строки
		INSERT INTO core.dim_staff
		(
			staff_id, 
			first_name, 
			last_name, 
			address, 
			district, 
			city_name,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			s.staff_id,
			s.first_name,
			s.last_name,
			ad.address,
			ad.district,
			ct.city as city_name,
			s.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active
		from
			staging.staff s (staff_id)
			join staging.store st using(store_id)
			join staging.address ad using (address_id)
			join staging.city ct using (city_id)
			left join new_staff_id_list idl using(staff_id)
		where 
			idl.staff_id is null
			and s.deleted is null;
	end;
$$ language plpgsql;

create or replace procedure core.load_payment()
as $$
	begin
		delete from core.fact_payment;
	
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk
		from
			staging.payment p
			join staging.rental r using (rental_id)
			join core.dim_inventory di 
				on r.inventory_id = di.inventory_id
				and p.payment_date between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds on p.staff_id = ds.staff_id
			join core.dim_date dt on dt.date_actual = p.payment_date::date;

	end;
$$ language plpgsql;

create or replace procedure core.load_rental()
as $$
	begin 
		-- отмечаем, что удаленные строки более не активны
		update core.fact_rental r
		set
			is_active = false,
			effective_date_to = sr.deleted 
		from 
			staging.rental sr
		where 
			sr.rental_id = r.rental_id 
			and sr.deleted is not null
			and r.is_active is true;
	
		-- получаем список идентификаторов новых фактов сдачи в аренду
		create temporary table new_rental_id_list on commit drop as 
		select
			r.rental_id 
		from
			staging.rental r
			left join core.fact_rental dr using(rental_id)
		where 
			dr.rental_id is null;
		
		-- вставляем новые факты сдачи в аренду
		insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dt_rental.date_dim_pk as rental_date_fk,
			dt_return.date_dim_pk as return_date_fk,
			r.last_update as effective_date_from,
			coalesce(r.deleted, '9999-01-01'::date) as effective_date_to,
			r.deleted is null as is_active
		from
			new_rental_id_list idl
			join staging.rental r
				on idl.rental_id = r.rental_id 
			join core.dim_inventory i 
				on r.inventory_id = i.inventory_id 
				and r.rental_date between i.effective_date_from and i.effective_date_to 
			join core.dim_staff s 
				on s.staff_id = r.staff_id
				and r.rental_date between s.effective_date_from and s.effective_date_to 
			join core.dim_date dt_rental on dt_rental.date_actual = r.rental_date::date
			left join core.dim_date dt_return on dt_return.date_actual = r.return_date::date;

		
		-- получаем список фактов сдачи в аренду, по которым была только проставлена дата возврата
		create temporary table update_return_date_id_list on commit drop as
		select
			r.rental_id
		from 
			staging.rental r 
			join core.fact_rental fr using(rental_id)
			join core.dim_inventory di on fr.inventory_fk = di.inventory_pk 
			join core.dim_staff ds on ds.staff_pk = fr.staff_fk 
			join core.dim_date dd on fr.rental_date_fk = dd.date_dim_pk 
			left join new_rental_id_list idl on idl.rental_id = r.rental_id 
		where 
			r.return_date is not null
			and fr.return_date_fk is null
			and fr.is_active is true
			and di.inventory_id = r.inventory_id 
			and ds.staff_id = r.staff_id 
			and dd.date_actual = r.rental_date::date
			and r.deleted is null
			and idl.rental_id is null;
			
		-- проставляем дату возврата у фактов сдачи в аренду, у которых была только проставлена дата возврата
		update core.fact_rental r
		set 
			return_date_fk = rd.date_dim_pk 
		from 
			staging.rental sr
			join update_return_date_id_list uidl using(rental_id)
			join core.dim_date rd on rd.date_actual = sr.return_date::date
		where 
			r.rental_id = sr.rental_id 
			and r.is_active is true;
		
		-- помечаем измененные факты сдачи в аренду не активными
		update core.fact_rental r
		set
			is_active = false,
			effective_date_to = sr.last_update 
		from
			staging.rental sr
			left join update_return_date_id_list uidl using(rental_id)
			left join new_rental_id_list idl using(rental_id)
		where 
			sr.rental_id = r.rental_id 
			and r.is_active is true
			and uidl.rental_id is null
			and idl.rental_id is null
			and sr.deleted is null;
		
		-- по измененным фактам сдачи в аренду добавляем новые актуальные строки
		insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dt_rental.date_dim_pk as rental_date_fk,
			dt_return.date_dim_pk as return_date_fk,
			r.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active
		from
			staging.rental r
			join core.dim_inventory i 
				on r.inventory_id = i.inventory_id 
				and r.rental_date between i.effective_date_from and i.effective_date_to 
			join core.dim_staff s 
				on s.staff_id = r.staff_id
				and r.rental_date between s.effective_date_from and s.effective_date_to 
			join core.dim_date dt_rental on dt_rental.date_actual = r.rental_date::date
			left join core.dim_date dt_return on dt_return.date_actual = r.return_date::date
			left join new_rental_id_list idl on r.rental_id = idl.rental_id
			left join update_return_date_id_list uidl on r.rental_id = uidl.rental_id
		where
			r.deleted is null
			and idl.rental_id is null
			and uidl.rental_id is null;
	end;
$$ language plpgsql;


-- создание data mart слоя

drop table if exists report.sales_date;

create table report.sales_date (
	date_title varchar(20) not null,
	amount numeric(7,2) not null,
	date_sort integer not null
);

drop table if exists report.sales_film;

create table report.sales_film (
	film_title varchar(255) not null,
	amount numeric(7,2) not null 
);


create or replace procedure report.sales_date_calc()
as $$
	begin 
		delete from report.sales_date;
	
		insert
			into
			report.sales_date
		(
			date_title, --'1 сентября 2022'
			amount,
			date_sort
		)
		select
			dt.day_of_month || ' ' || dt.month_name || ' ' || dt.year_actual as date_title,
			sum(fp.amount) as amount,
			dt.date_dim_pk as date_sort
		from
			core.fact_payment fp
			join core.dim_date dt
				on	 fp.payment_date_fk = dt.date_dim_pk
		group by
			dt.day_of_month || ' ' || dt.month_name || ' ' || dt.year_actual,
			dt.date_dim_pk;

	end
$$ language plpgsql;

create or replace procedure report.sales_film_calc()
as $$
	begin 
		delete from report.sales_film;
	
		INSERT INTO report.sales_film
		(
			film_title, 
			amount
		)
		select
			di.title as film_title,
			sum(p.amount) as amount
		from
			core.fact_payment p
			join core.dim_inventory di 
				on p.inventory_fk = di.inventory_pk 
		group by
			di.title;
	end;
$$ language plpgsql;


