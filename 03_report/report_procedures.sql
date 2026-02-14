-- Процедура наполнения продаж по дате
create or replace procedure report.sales_date_calc()
as $$
begin 
    delete from report.sales_date;

    insert into report.sales_date
    (
        date_title, 
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
            on fp.payment_date_fk = dt.date_dim_pk
    group by
        dt.day_of_month || ' ' || dt.month_name || ' ' || dt.year_actual,
        dt.date_dim_pk;

end
$$ language plpgsql;


-- Процедура наполнения продаж по фильму
create or replace procedure report.sales_film_calc()
as $$
begin 
    delete from report.sales_film;

    insert into report.sales_film
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

end
$$ language plpgsql;
