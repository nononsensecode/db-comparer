INSERT INTO employee (id, name, age, joined, details, positions, family) VALUES (1, 'John', 30, '2019-01-01 00:00:00', '{"address": "123 Main St","city": "New York","state": "NY","zip": "10001"}', '[{"title": "Manager","department": "Sales","salary": 100000},{"title": "Associate","department": "Sales","salary": 50000}]', '[{"spouse": {"name": "Jane","age": 25},"children": [{"name": "Sally","age": 5},{"name": "Mike","age": 3}]}]');
INSERT INTO employee (id, name, age, joined, details, positions, family) VALUES (2, 'Jane', 25, '2019-02-01 00:00:00', '{"address": "456 Main St","city": "New York","state": "NY","zip": "10001"}', '[{"title": "Associate","department": "Sales","salary": 50000}]', '[{"spouse": {"name": "John","age": 30},"children": [{"name": "Sally","age": 5},{"name": "Mike","age": 3}]}]');

---- create above / drop below ----

TRUNCATE TABLE employee;