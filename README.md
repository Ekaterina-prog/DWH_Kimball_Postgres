Архитектура DWH (Kimball, PostgreSQL)

1. Source Layer

Источник данных — PostgreSQL база dvdrental.

Подключение реализовано через postgres_fdw.
Данные импортированы в схему film_src.

Source Layer не содержит изменений бизнес-логики — это отражение структуры операционной системы.

2. Staging Layer

Схема: staging

Назначение:

промежуточное хранение данных

минимальная техническая обработка

подготовка к загрузке в Core слой

реализация инкрементальной загрузки

Особенности:

Полная загрузка для большинства таблиц

Инкрементальная загрузка для inventory, rental, payment

Поддержка Soft Delete через поле deleted

Контроль загрузки через таблицу staging.last_update

Централизация логики времени загрузки:

staging.get_last_update_table(table_name)

staging.set_table_load_time(table_name, current_update_dt)

Staging слой:

не содержит surrogate keys

не содержит бизнес-логики

отражает структуру источника с техническими расширениями (last_update, deleted)

Реализованные таблицы:

film

inventory

rental

payment

staff

address

city

store

3. Core Layer (Star Schema)

Схема: core
Реализована звездная схема по методологии Kimball.

Факты используют surrogate keys измерений.

Измерения (Dimensions)
dim_date

Календарное измерение, генерируется процедурой core.load_date.

Содержит:

день, месяц, квартал, год

ISO week

week_of_year

first/last day of period

признак выходного дня

dim_inventory (SCD Type 2)

Измерение реализует Slowly Changing Dimension Type 2.

Поддержка историзации:

effective_date_from

effective_date_to

is_active

Особенность:
Измерение денормализовано — включает атрибуты из film:

title

rental_duration

rental_rate

length

rating

Изменение параметров фильма приводит к созданию новой версии строки в dim_inventory, даже если inventory_id не менялся.

Это обеспечивает историческую корректность фактов.

dim_staff

Измерение сотрудников.

Стратегия загрузки:

Полная перезагрузка (без SCD)

4. Факты (Facts)
   fact_rental

Факт аренды.

Содержит:

rental_id

inventory_fk

staff_fk

rental_date_fk

return_date_fk

Реализована историчность (SCD Type 2):

Поля:

effective_date_from

effective_date_to

is_active

Новая версия создается при изменении:

staff_id

inventory_id

rental_date

return_date (если уже была и изменилась)

Если return_date была NULL и заполнилась впервые — выполняется UPDATE без создания новой версии.

fact_payment

Факт платежей.

Содержит:

payment_id

amount

rental_id

payment_date_fk

inventory_fk

staff_fk

Реализована:

инкрементальная загрузка

поддержка Soft Delete

историчность (SCD Type 2)

Историчность поддерживается по бизнес-полям:

amount

rental_id

payment_date

Если изменяется хотя бы одно из этих полей:

текущая версия строки закрывается

создается новая версия записи

Если изменяются только surrogate keys измерений (inventory_fk, staff_fk):

выполняется UPDATE без создания новой версии

Историчность в фактах реализована для поддержки аудита бизнес-событий, а не изменений измерений.

5. Data Mart Layer

Схема: report

Реализованы агрегированные витрины:

report.sales_date — продажи по датам

report.sales_film — продажи по фильмам

Процедуры:

report.sales_date_calc()

report.sales_film_calc()

6. Стратегия загрузки

Используется гибридная стратегия:

Инкрементальная загрузка для:

staging.inventory

staging.rental

staging.payment

SCD Type 2 для:

core.dim_inventory

core.fact_rental

core.fact_payment

Полная загрузка для:

dim_staff

film (справочник)

Генерация календаря через отдельную процедуру

Единое время загрузки

В full_load() используется единый параметр:

declare current_update_dt timestamp = now();

Это время передается во все процедуры staging слоя.

Преимущество:

устраняется рассинхронизация данных

обеспечивается консистентность среза загрузки

7. Практическая ценность архитектуры

Реализованная модель обеспечивает:

инкрементальную загрузку без полной очистки фактов

поддержку Soft Delete

историчность измерений и фактов

корректную связь факта с версией измерения

аудит изменений бизнес-событий

воспроизводимость отчетов на разные даты построения

Фактовые таблицы больше не требуют полной перезагрузки, что снижает нагрузку и увеличивает масштабируемость решения.
