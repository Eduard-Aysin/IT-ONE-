-- 1. Создаем таблицу
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    event_type VARCHAR(50),
    created_at TIMESTAMP
);

-- 2. Наполняем данными (100 000 строк)
INSERT INTO events (user_id, event_type, created_at)
SELECT 
    -- Генерируем user_id от 1 до 1000
    floor(random() * 1000 + 1)::int as user_id,
    
    -- Выбираем один из 10 типов событий случайным образом
    (ARRAY['login', 'logout', 'view_page', 'click_ad', 'add_to_cart', 
           'purchase', 'search', 'filter_apply', 'error', 'subscribe'])[floor(random() * 10 + 1)] as event_type,
    
    -- Распределяем даты в течение последнего года
    now() - (random() * interval '365 days') as created_at
FROM generate_series(1, 100000);

-- Проверить количество строк
SELECT count(*) FROM events;

-- Проверить распределение по типам событий
SELECT event_type, count(*) 
FROM events 
GROUP BY event_type;

-- Посмотреть первые 5 строк
SELECT * FROM events LIMIT 5;

EXPLAIN ANALYZE SELECT * FROM events WHERE user_id = 123
create index user_id on events(user_id)

create index type_create on events(event_type, created_at)
EXPLAIN analyze SELECT * FROM events WHERE created_at > '2026-04-13 13:52:32.853'


CREATE TABLE events_partitioned (
    id SERIAL,
    user_id INTEGER,
    event_type VARCHAR(50),
    created_at TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);


-- Секция на март
CREATE TABLE events_2026_03 PARTITION OF events_partitioned
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

-- Секция на апрель (сюда попадет ваш пример 2026-04-13)
CREATE TABLE events_2026_04 PARTITION OF events_partitioned
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

-- Секция на май
CREATE TABLE events_2026_05 PARTITION OF events_partitioned
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

INSERT INTO events_partitioned (user_id, event_type, created_at)
SELECT user_id, event_type, created_at
FROM events
WHERE created_at >= '2026-03-01' 
  AND created_at <  '2026-06-01';

explain SELECT * FROM events_partitioned 
WHERE created_at BETWEEN '2026-04-13 00:00:00' AND '2026-04-14 00:00:00';

insert into events_partitioned (user_id, event_type, created_at)
values (12345, 'new_event', '2027-04-13 00:00:00')

ALTER TABLE events ADD COLUMN metadata JSONB;

UPDATE events
SET metadata = jsonb_build_object(
    'status', 'active',
    'priority', (user_id % 3) + 1
);

-- Где приоритет 3, статус меняем на disabled
UPDATE events 
SET metadata = metadata || '{"status": "disabled"}'::jsonb
WHERE (metadata->>'priority')::int = 3;

-- Где приоритет 2, статус меняем на new
UPDATE events 
SET metadata = metadata || '{"status": "new"}'::jsonb
WHERE (metadata->>'priority')::int = 2;

CREATE INDEX metadata_gin ON events USING GIN (metadata);
CREATE INDEX idx_metadata_status_text ON events ((metadata->>'status'));

explain analyze select * from events WHERE (metadata->>'status' = 'active');


