CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    joined TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details JSON NOT NULL,
    positions JSON NOT NULL,
    family JSON NOT NULL,
    emp_id uuid NOT NULL
);

---- create above / drop below ----

DROP TABLE employee;