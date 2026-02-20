-- создание процедур ref слоя

create or replace procedure film_id_sync()
as $$
	begin 
		insert into ref.film (
			film_nk
		)
		select 
			f.film_id 
		from 
			ods.film f 
			left join ref.film rf
				on f.film_id =rf.film_nk
			where 
				rf.film_nk is null
			order by 
				f.film_id ;
	end;
$$ language plpgsql;

create or replace procedure inventory_id_sync()
as $$
	begin 
		insert into ref.inventory (
			inventory_nk
		)
		select 
			i.inventory_id 
		from 
			ods.inventory i
			left join ref.inventory ri
				on i.inventory_id =ri.inventory_nk
			where 
				ri.inventory_nk is null
			order by 
				i.inventory_id;
	end;
$$ language plpgsql;

create or replace procedure rental_id_sync()
as $$
	begin 
		insert into ref.rental (
			rental_nk
		)
		select 
			r.rental_id 
		from 
			ods.rental r 
			left join ref.rental rr
				on r.rental_id =rr.rental_nk
			where 
				rr.rental_nk is null
			order by 
				r.rental_id;
	end;
$$ language plpgsql;

create or replace procedure address_id_sync()
as $$
	begin 
		insert into ref.address (
			address_nk
		)
		select 
			a.address_id 
		from 
			ods.address a 
			left join ref.address ra
				on a.address_id =ra.address_nk
			where 
				ra.address_nk is null
			order by 
				a.address_id;
	end;
$$ language plpgsql;


create or replace procedure city_id_sync()
as $$
	begin 
		insert into ref.city (
			city_nk
		)
		select 
			c.city_id 
		from 
			ods.city c 
			left join ref.city rc
				on c.city_id =rc.city_nk
			where 
				rc.city_nk is null
			order by 
				c.city_id;
	end;
$$ language plpgsql;


create or replace procedure staff_id_sync()
as $$
	begin 
		insert into ref.staff (
			staff_nk
		)
		select 
			s.staff_id 
		from 
			ods.staff s
			left join ref.staff rs
				on s.staff_id =rs.staff_nk
			where 
				rs.staff_nk is null
			order by 
				s.staff_id;
	end;
$$ language plpgsql;


create or replace procedure store_id_sync()
as $$
	begin 
		insert into ref.store (
			store_nk
		)
		select 
			s.store_id 
		from 
			ods.store s 
			left join ref.store rs
				on s.store_id =rs.store_nk
			where 
				rs.store_nk is null
			order by 
				s.store_id;
	end;
$$ language plpgsql;

create or replace procedure payment_id_sync()
as $$
	begin 
		insert into ref.payment (
			payment_nk
		)
		select 
			p.payment_id 
		from 
			ods.payment p
			left join ref.payment rp
				on p.payment_id =rp.payment_nk
			where 
				rp.payment_nk is null
			order by 
				p.payment_id;
	end;
$$ language plpgsql;