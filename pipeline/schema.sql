-- This file should contain all code required to create & seed database tables.
DROP TABLE IF EXISTS special_requests;
DROP TABLE IF EXISTS rating;
DROP TABLE IF EXISTS exhibitions;
DROP TABLE IF EXISTS score;
DROP TABLE IF EXISTS help_type;
DROP TABLE IF EXISTS floors;
DROP TABLE IF EXISTS departments;



CREATE TABLE score (
    score_id SMALLINT PRIMARY KEY,
    score_name VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO score(score_id, score_name) 
VALUES (0, 'Terrible'), (1, 'Bad'), (2, 'Neutral'), (3, 'Good'), (4, 'Amazing');


CREATE TABLE help_type (
    help_type_id SMALLINT PRIMARY KEY,
    help_type_name VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO help_type(help_type_id, help_type_name) 
VALUES (0, 'Assistance'), (1, 'Emergency');

CREATE TABLE floors (
    floor_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    floor_number VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO floors(floor_number) VALUES ('Vault'), ('1'), ('2'), ('3');


CREATE TABLE departments (
    department_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    department_name VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO departments(department_name) 
VALUES ('Entomology'),('Geology'), ('Paleontology'), ('Zoology'), ('Ecology');



CREATE TABLE exhibitions (
    exhibition_id SMALLINT NOT NULL PRIMARY KEY,
    exhibition_name VARCHAR(100) NOT NULL,
    floor_id SMALLINT NOT NULL,
    department_id INT NOT NULL,
    start_date DATE CHECK (start_date <= CURRENT_DATE),
    description TEXT,
    FOREIGN KEY (floor_id) REFERENCES floors(floor_id),
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);


CREATE TABLE rating (
    rating_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    score_id SMALLINT NOT NULL,
    exhibition_id SMALLINT NOT NULL,
    date TIMESTAMP CHECK (date <= NOW()),
    FOREIGN KEY (score_id) REFERENCES score(score_id),
    FOREIGN KEY (exhibition_id) REFERENCES exhibitions(exhibition_id)
);

CREATE TABLE special_requests (
    request_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    help_type_id SMALLINT NOT NULL,
    exhibition_id SMALLINT NOT NULL,
    date TIMESTAMP CHECK (date <= NOW()),
    FOREIGN KEY (help_type_id) REFERENCES help_type(help_type_id),
    FOREIGN KEY (exhibition_id) REFERENCES exhibitions(exhibition_id)
);