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
