-- Создание новой схемы для отчетов

create schema report;

-- Удаление таблиц если существуют
drop table if exists report.sales_date;
drop table if exists report.sales_film;

-- Таблица продаж по дате
create table report.sales_date (
    date_title varchar(20) not null,
    amount numeric(7,2) not null,
    date_sort integer not null
);

-- Таблица продаж по фильму
create table report.sales_film (
    film_title varchar(255) not null,
    amount numeric(7,2) not null 
);