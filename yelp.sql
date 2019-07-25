-- Business Table:
-- ID
-- Name
-- rating
-- price
-- Address (display addres):
-- -- Addr
-- -- City
-- -- ZIP
-- -- State

-- ALTER TABLE businesses
-- drop column address,
--     drop column city,
--     drop column zip,
--     drop column state
drop table businesses


CREATE TABLE IF NOT EXISTS businesses(
    id varchar(30) NOT NULL,
    name VARCHAR(40) NOT NULL,
    rating float,
    price varchar(5)
    PRIMARY KEY (id)
)  ENGINE=INNODB;

-- Review table:
-- ID
-- text
-- rating
-- creation_date
--

CREATE table if not exists reviews(
    id varchar(30) not null,
    text varchar(255),
    rating int,
    creation_date DATETIME,
    PRIMARY KEY (id),
    FOREIGN KEY fk_bus(id)
    REFERENCES businesses(id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
)



select * from businesses