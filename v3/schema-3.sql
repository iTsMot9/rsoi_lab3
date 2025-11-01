CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Cars table
CREATE TABLE cars (
    id SERIAL PRIMARY KEY,
    car_uid UUID UNIQUE NOT NULL DEFAULT uuid_generate_v4(),
    brand VARCHAR(80) NOT NULL,
    model VARCHAR(80) NOT NULL,
    registration_number VARCHAR(20) NOT NULL,
    power INT,
    price INT NOT NULL,
    type VARCHAR(20) CHECK (type IN ('SEDAN', 'SUV', 'MINIVAN', 'ROADSTER')),
    availability BOOLEAN NOT NULL
);

INSERT INTO cars (car_uid, brand, model, registration_number, power, price, type, availability)
VALUES ('109b42f3-198d-4c89-9276-a7520a7120ab', 'Mercedes Benz', 'GLA 250', 'ЛО777Х799', 249, 3500, 'SEDAN', true);

-- Rental table
CREATE TABLE rental (
    id SERIAL PRIMARY KEY,
    rental_uid UUID UNIQUE NOT NULL DEFAULT uuid_generate_v4(),
    username VARCHAR(80) NOT NULL,
    payment_uid UUID NOT NULL,
    car_uid UUID NOT NULL,
    date_from DATE NOT NULL,
    date_to DATE  NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'FINISHED', 'CANCELED'))
);

-- Payment table
CREATE TABLE payment (
    id SERIAL PRIMARY KEY,
    payment_uid UUID NOT NULL DEFAULT uuid_generate_v4(),
    status VARCHAR(20) NOT NULL CHECK (status IN ('PAID', 'CANCELED')),
    price INT NOT NULL
);
