INSERT INTO employee (id, name, age, joined, details) VALUES (1, 'John', 30, '2019-01-01 00:00:00', '{"address": "123 Main St","city": "New York","state": "NY","zip": "10001"}');
INSERT INTO employee (id, name, age, joined, details) VALUES (2, 'Jane', 25, '2019-02-01 00:00:00', '{"address": "456 Main St","city": "New York","state": "NY","zip": "10001"}');

---- create above / drop below ----

TRUNCATE TABLE employee;