-- Настройка FDW по Инмону: подключение к внешней БД dvdrental для загрузки данных в staging

-- Создание расширения postgres_fdw
create extension postgres_fdw;

-- Создание внешнего сервера для подключения к БД dvdrental
create server film_pg foreign data wrapper postgres_fdw
options (
    host 'localhost',
    dbname 'dvdrental',
    port '5432'
);

-- Созданием user mapping
create user mapping for postgres
server film_pg
options (
    user 'postgres',
    password '******'
);

-- Создаем схему 
drop schema if exists film_src;
create schema film_src authorization postgres;

-- Добавляем нестандартые типы данных
DROP TYPE if exists mpaa_rating;
CREATE TYPE public.mpaa_rating AS ENUM (
	'G',
	'PG',
	'PG-13',
	'R',
	'NC-17');

CREATE DOMAIN public.year AS integer CHECK(VALUE >= 1901 AND VALUE <= 2155);

-- Импорт внешних таблиц из источника
import foreign schema public from server film_pg into film_src;