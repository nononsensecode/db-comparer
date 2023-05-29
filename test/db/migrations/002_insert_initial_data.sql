INSERT INTO employee (id, name, age, joined, details, positions, family, emp_id) VALUES (1, 'John', 30, '2019-01-01 00:00:00', '{"address": "123 Main St","city": "New York","state": "NY","zip": "10001"}', '[{"title": "Manager","department": "Sales","salary": 100000},{"title": "Associate","department": "Sales","salary": 50000}]', '[{"spouse": {"name": "Jane","age": 25},"children": [{"name": "Sally","age": 5},{"name": "Mike","age": 3}]}]', '5b84bc6a-06ba-41a0-9421-c1e7f73fd845'::UUID);
INSERT INTO employee (id, name, age, joined, details, positions, family, emp_id) VALUES (2, 'Jane', 25, '2019-02-01 00:00:00', '{"address": "456 Main St","city": "New York","state": "NY","zip": "10001"}', '[{"title": "Associate","department": "Sales","salary": 50000}]', '[{"spouse": {"name": "John","age": 30},"children": [{"name": "Sally","age": 5},{"name": "Mike","age": 3}]}]', '68c7d4bb-0434-40a7-9329-a7d3b2b7c9ac'::UUID);

---- create above / drop below ----

TRUNCATE TABLE employee;