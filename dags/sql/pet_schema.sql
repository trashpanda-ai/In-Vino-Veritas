-- create pet table
DROP TABLE pet;
CREATE TABLE IF NOT EXISTS pet (
   
    name_ VARCHAR NOT NULL,
    pet_type VARCHAR NOT NULL,
    birth_date DATE NOT NULL,
    OWNER_ VARCHAR NOT NULL);