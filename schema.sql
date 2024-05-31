-- -----------------------------------------------------
-- Table TABLE_LIST
-- -----------------------------------------------------
CREATE TABLE TABLE_LIST(
	table_name VARCHAR(100) NOT NULL,
	is_scanned VARCHAR(1),
	PRIMARY KEY(table_name)
);


-- -----------------------------------------------------
-- Table ATTR
-- -----------------------------------------------------
CREATE TABLE ATTR(
	table_name VARCHAR(100) NOT NULL,
	attr_name VARCHAR(100) NOT NULL,
	data_type VARCHAR(100),
	null_count INT,
	record_count INT,
	distinct_count INT,
	is_candidate VARCHAR(1),
	is_numeric VARCHAR(1),
	PRIMARY KEY(table_name, attr_name),
	FOREIGN KEY(table_name)
        REFERENCES TABLE_LIST(table_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);


-- -----------------------------------------------------
-- Table CATEGORICAL_ATTR
-- -----------------------------------------------------
CREATE TABLE CATEGORICAL_ATTR(
	table_name VARCHAR(100) NOT NULL,
	attr_name VARCHAR(100) NOT NULL,
	symbol_count INT,
	PRIMARY KEY(table_name, attr_name),
	FOREIGN KEY(table_name, attr_name)
        REFERENCES ATTR(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);


-- -----------------------------------------------------
-- Table NUMERIC_ATTR
-- -----------------------------------------------------
CREATE TABLE NUMERIC_ATTR(
	table_name VARCHAR(100) NOT NULL,
	attr_name VARCHAR(100) NOT NULL,
	zero_count INT,
	min_value DOUBLE,
	max_value DOUBLE,
	PRIMARY KEY(table_name, attr_name),
	FOREIGN KEY(table_name, attr_name)
        REFERENCES ATTR(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);


-- -----------------------------------------------------
-- Table STD_REPR_ATTR
-- -----------------------------------------------------
CREATE TABLE STD_REPR_ATTR(
	repr_attr_id INT NOT NULL AUTO_INCREMENT,
	repr_attr_name VARCHAR(100),
	PRIMARY KEY(repr_attr_id)
);

INSERT INTO STD_REPR_ATTR VALUES(01,'학업정보');
INSERT INTO STD_REPR_ATTR VALUES(02,'금융정보');
INSERT INTO STD_REPR_ATTR VALUES(03,'회원정보');
INSERT INTO STD_REPR_ATTR VALUES(04,'건강정보');


-- -----------------------------------------------------
-- Table REPR_ATTR
-- -----------------------------------------------------
CREATE TABLE REPR_ATTR(
	table_name VARCHAR(100) NOT NULL,
	attr_name VARCHAR(100) NOT NULL,
	repr_attr_id INT,
	PRIMARY KEY(table_name, attr_name),
	FOREIGN KEY(table_name, attr_name)
        REFERENCES ATTR(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
	FOREIGN KEY(repr_attr_id) 
        REFERENCES STD_REPR_ATTR(repr_attr_id) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE

);


-- -----------------------------------------------------
-- Table STD_JOIN_KEY
-- -----------------------------------------------------
CREATE TABLE STD_JOIN_KEY(
	key_id INT NOT NULL AUTO_INCREMENT,
	key_name VARCHAR(100),
	PRIMARY KEY(key_id)
);

INSERT INTO STD_JOIN_KEY VALUES(01,'주민등록번호');
INSERT INTO STD_JOIN_KEY VALUES(02,'전화번호');
INSERT INTO STD_JOIN_KEY VALUES(03,'차량번호');
INSERT INTO STD_JOIN_KEY VALUES(04,'이메일 주소');
INSERT INTO STD_JOIN_KEY VALUES(05,'IP');


-- -----------------------------------------------------
-- Table JOIN_KEY
-- -----------------------------------------------------
CREATE TABLE JOIN_KEY(
	table_name VARCHAR(100) NOT NULL,
	attr_name VARCHAR(100) NOT NULL,
	join_key_id INT,
	PRIMARY KEY(table_name, attr_name),
	FOREIGN KEY(table_name, attr_name) 
        REFERENCES ATTR(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
	FOREIGN KEY(join_key_id) 
        REFERENCES STD_JOIN_KEY(key_id) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);


-- -----------------------------------------------------
-- Table SINGLE_JOIN_TABLE_LIST
-- -----------------------------------------------------
CREATE TABLE SINGLE_JOIN_TABLE_LIST(
	id INT,
	source_table_name VARCHAR(100) NOT NULL,
    source_record_count INT,
    source_attr_name VARCHAR(100) NOT NULL,
	target_table_name VARCHAR(100) NOT NULL,
    target_record_count INT,
	target_attr_name VARCHAR(100) NOT NULL,
	joined_table_name VARCHAR(100) NOT NULL,
    join_key VARCHAR(100),
	joined_record_count INT,
    source_success_rate DECIMAL(3,2),
    target_success_rate DECIMAL(3,2),
    is_complete INT,
    join_status VARCHAR(100),
	PRIMARY KEY(id),
	FOREIGN KEY(source_table_name, source_attr_name) 
        REFERENCES JOIN_KEY(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
	FOREIGN KEY(target_table_name, target_attr_name) 
        REFERENCES JOIN_KEY(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);


-- -----------------------------------------------------
-- Table MULTIPLE_JOIN_TABLE_LIST
-- -----------------------------------------------------
CREATE TABLE MULTIPLE_JOIN_TABLE_LIST(
	id INT,
    inner_id INT,
    source_table_name VARCHAR(100) NOT NULL,
    source_record_count INT,
    source_attr_name VARCHAR(100) NOT NULL,
	target_table_name VARCHAR(100) NOT NULL,
    target_record_count INT,
	target_attr_name VARCHAR(100) NOT NULL,
	joined_table_name VARCHAR(100) NOT NULL,
    join_key VARCHAR(100),
	joined_record_count INT,
    source_success_rate DECIMAL(3,2),
    target_success_rate DECIMAL(3,2),
    is_complete INT,
    join_status VARCHAR(100),
	PRIMARY KEY(id, inner_id),
	FOREIGN KEY(source_table_name, source_attr_name) 
        REFERENCES JOIN_KEY(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
	FOREIGN KEY(target_table_name, target_attr_name) 
        REFERENCES JOIN_KEY(table_name, attr_name) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE
);