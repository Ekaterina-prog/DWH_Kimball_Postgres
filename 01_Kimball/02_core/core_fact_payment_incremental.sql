create or replace procedure core.load_payment()
as $$
	begin
		-- отмечаем, что удаленные строки более не активны
		update core.fact_payment p
		set
			is_active = false,
			effective_date_to = sp.deleted 
		from 
			staging.payment sp
		where 
			sp.payment_id = p.payment_id 
			and sp.deleted is not null
			and p.is_active is true;
		
		-- получаем список идентификаторов новых платежей
		create temporary table new_payment_id_list on commit drop as 
		select
			p.payment_id 
		from
			staging.payment p
			left join core.fact_payment fp using(payment_id)
		where 
			fp.payment_id is null;
		
		-- вставляем новые платежи
		
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk,
			rental_id,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk,
			p.rental_id,
			'1900-01-01'::date effective_date_from,
			coalesce(p.deleted, '9999-01-01'::date) as effective_date_to,
			p.deleted is null as is_active
		from
			staging.payment p
			join new_payment_id_list np using (payment_id)
			join core.dim_inventory di 
				on p.inventory_id = di.inventory_id 
				and p.last_update between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds 
				on p.staff_id = ds.staff_id
				and p.last_update between ds.effective_date_from and ds.effective_date_to 
			join core.dim_date dt on dt.date_actual = p.payment_date::date;
		
		-- получаем список платежей, по которым не было изменений по полям, по которым мы поддерживаем историчность
		
		create temporary table updated_payments_wo_history on commit drop as 
		select 
			p.payment_id 
		from
			staging.payment p 
			join core.fact_payment fp 	
				on p.payment_id = fp.payment_id 
				and p.last_update between fp.effective_date_from and fp.effective_date_to 
			join core.dim_date dd 
				on dd.date_dim_pk = fp.payment_date_fk 
		where 
			p.amount = fp.amount 
			and p.payment_date::date = dd.date_actual 
			and p.rental_id = fp.rental_id;
		
		-- проставляем новые значения полей по измененным платежам, по которым не нужна историчность
		
		update core.fact_payment fp
		set
			inventory_fk = di.inventory_pk,
			staff_fk = ds.staff_pk 
		from
			updated_payments_wo_history pwoh
			join staging.payment p
				on p.payment_id = pwoh.payment_id
			join core.dim_inventory di 
				on p.inventory_id = di.inventory_id 
				and p.last_update between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds 
				on p.staff_id = ds.staff_id
				and p.last_update between ds.effective_date_from and ds.effective_date_to 
		where 
			p.payment_id = fp.payment_id
			and p.last_update between fp.effective_date_from and fp.effective_date_to;	
		
		-- помечаем платежи, по изменениям которых нужно реализовать историчность, не активными
		
		update core.fact_payment fp
		set
			is_active = false,
			effective_date_to = p.last_update
		from
			staging.payment p
			left join updated_payments_wo_history pwoh
				on p.payment_id = pwoh.payment_id
			left join new_payment_id_list np
				on p.payment_id = np.payment_id
		where 
			p.payment_id = fp.payment_id 
			and fp.is_active is true
			and pwoh.payment_id is null
			and p.deleted is null
			and np.payment_id is null;
		
		-- по измененным платежам, по котоырм нужна историчность, добавляем новые актуальные строки
		
		insert into core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk,
			inventory_fk,
			staff_fk,
			rental_id,
			effective_date_from,
			effective_date_to,
			is_active
		)
		select
			p.payment_id,
			p.amount,
			dt.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk,
			p.rental_id,
			p.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active
		from
			staging.payment p
			left join updated_payments_wo_history pwoh using (payment_id)
			left join new_payment_id_list np using (payment_id)
			join core.dim_inventory di 
				on p.inventory_id = di.inventory_id 
				and p.last_update between di.effective_date_from and di.effective_date_to 
			join core.dim_staff ds 
				on p.staff_id = ds.staff_id
				and p.last_update between ds.effective_date_from and ds.effective_date_to 
			join core.dim_date dt on dt.date_actual = p.payment_date::date
		where 
			pwoh.payment_id is null
			and np.payment_id is null
			and p.deleted is null;

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
			'1900-01-01'::date effective_date_from,
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