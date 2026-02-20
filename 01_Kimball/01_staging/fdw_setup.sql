-- Настройка FDW: подключение к внешней БД dvdrental для загрузки данных в staging

-- Создание расширения postgres_fdw
create extension if not exists postgres_fdw;

-- Создание внешнего сервера для подключения к БД dvdrental
create server dvd_rental_server
foreign data wrapper postgres_fdw
options (
    host 'localhost',
    dbname 'dvdrental',
    port '5432'
);

-- Настройка пользовательского отображения (аутентификация)
create user mapping for current_user
server dvd_rental_server
options (
    user 'postgres',
    password '******'
);

-- Создание схемы для внешних таблиц
create schema if not exists film_src authorization postgres;

-- Создание пользовательских типов для корректной работы FDW
create type mpaa_rating as enum (
    'G',
    'PG',
    'PG-13',
    'R',
    'NC-17'
);

create domain year as integer;

-- Импорт внешних таблиц из источника
import foreign schema public
from server film_pg
into film_src;