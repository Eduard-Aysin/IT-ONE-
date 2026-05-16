-- Создаем таблицу с нужными полями
CREATE TABLE IF NOT EXISTS web_logs (
    EventDate Date,
    EventTime DateTime,
    URL String,
    UserID UInt64,
    StatusCode UInt16,
    ResponseTime Float32
) 
ENGINE = MergeTree() 
PARTITION BY toYYYYMM(EventDate)
ORDER BY (EventDate, URL);

-- Наполняем её данными (10 млн строк)
INSERT INTO web_logs
SELECT 
    toDate('2026-05-01') + number % 10 AS EventDate,             -- Данные за 10 дней мая
    toDateTime('2026-05-01 00:00:00') + number AS EventTime,
    'https://myshop.ru/products/' || toString(number % 50) AS URL, -- 50 разных страниц
    rand64() % 100000 AS UserID,                                  -- 100к уникальных пользователей
    -- Генерируем статус-коды: 90% это 200, остальные 404 и 500
    multiIf(number % 10 < 8, 200, number % 10 = 8, 404, 500) AS StatusCode,
    randCanonical() * 1.5 AS ResponseTime                       -- Время ответа 0-1.5 сек
FROM numbers(10000000)
SETTINGS max_insert_threads = 2;


--Тестовй запрос посмтреть структуру
select * from web_logs wl limit 50

--5. Анализ данных:
  --– Выполните следующие запросы для анализа данных:
  
--Найдите общее количество запросов за каждый день.
select 
	EventDate,
	COUNT(*) as total_day_responses
from web_logs wl
group by EventDate

--Определите среднее время ответа для каждого URL.
select 
	URL,
	AVG(ResponseTime) as avg_response
from web_logs wl
group by URL

--Подсчитайте количество запросов с ошибками (например, статус-коды 4xx и 5xx).
--в разбивке на коды
select 
	StatusCode, 
	count() as erors_count
from web_logs
where StatusCode >= 400
group by StatusCode


--общее кол-во ошибок
select count() as total_errors
from web_logs
where StatusCode >= 400

--Найдите топ-10 пользователей по количеству запросов.
select 
	UserID,
	count(EventTime) as total_user_count
from web_logs wl
group by UserID
order by total_user_count desc
limit 10


--смотрим как отрабатывает запрос
EXPLAIN PLAN actions = 1, indexes = 1, projections = 1--PIPELINE --ESTIMATE
select count() as total_errors
from web_logs
where StatusCode >= 400

-- по плану видно что читается вся таблица

ALTER TABLE web_logs
    ADD INDEX idx_status_code StatusCode TYPE minmax GRANULARITY 4;

ALTER TABLE web_logs
    MATERIALIZE INDEX idx_status_code;

-- смотрим повторно
EXPLAIN PLAN actions = 1, indexes = 1, projections = 1--PIPELINE --ESTIMATE
select count() as total_errors
from web_logs
where StatusCode >= 400

-- запрос ускорился  - Granules: 254/1222, было 1222/1222 (вся таблица)





