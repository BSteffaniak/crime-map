CREATE TABLE crime_categories (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    parent_id   INTEGER REFERENCES crime_categories(id),
    severity    SMALLINT NOT NULL CHECK (severity BETWEEN 1 AND 5),
    nibrs_code  TEXT,
    ucr_part    SMALLINT CHECK (ucr_part IN (1, 2)),

    CONSTRAINT uq_category_name UNIQUE (name)
);

-- Seed top-level categories
INSERT INTO crime_categories (name, severity) VALUES
    ('VIOLENT', 4),
    ('PROPERTY', 3),
    ('DRUG_NARCOTICS', 3),
    ('PUBLIC_ORDER', 2),
    ('FRAUD_FINANCIAL', 2),
    ('OTHER', 1);

-- Seed subcategories (parent_id references inserted above)
INSERT INTO crime_categories (name, parent_id, severity, nibrs_code) VALUES
    -- Violent
    ('HOMICIDE',             (SELECT id FROM crime_categories WHERE name = 'VIOLENT'), 5, '09A'),
    ('SEXUAL_ASSAULT',       (SELECT id FROM crime_categories WHERE name = 'VIOLENT'), 5, '11A'),
    ('ROBBERY',              (SELECT id FROM crime_categories WHERE name = 'VIOLENT'), 4, '120'),
    ('AGGRAVATED_ASSAULT',   (SELECT id FROM crime_categories WHERE name = 'VIOLENT'), 4, '13A'),
    ('SIMPLE_ASSAULT',       (SELECT id FROM crime_categories WHERE name = 'VIOLENT'), 3, '13B'),
    -- Property
    ('BURGLARY',             (SELECT id FROM crime_categories WHERE name = 'PROPERTY'), 3, '220'),
    ('LARCENY_THEFT',        (SELECT id FROM crime_categories WHERE name = 'PROPERTY'), 2, '23A'),
    ('MOTOR_VEHICLE_THEFT',  (SELECT id FROM crime_categories WHERE name = 'PROPERTY'), 3, '240'),
    ('ARSON',                (SELECT id FROM crime_categories WHERE name = 'PROPERTY'), 4, '200'),
    ('VANDALISM',            (SELECT id FROM crime_categories WHERE name = 'PROPERTY'), 2, '290'),
    -- Drug/Narcotics
    ('DRUG_POSSESSION',            (SELECT id FROM crime_categories WHERE name = 'DRUG_NARCOTICS'), 2, '35A'),
    ('DRUG_SALES_MANUFACTURING',   (SELECT id FROM crime_categories WHERE name = 'DRUG_NARCOTICS'), 3, '35A'),
    ('DRUG_EQUIPMENT',             (SELECT id FROM crime_categories WHERE name = 'DRUG_NARCOTICS'), 1, '35B'),
    -- Public Order
    ('WEAPONS_VIOLATION',    (SELECT id FROM crime_categories WHERE name = 'PUBLIC_ORDER'), 3, '520'),
    ('DUI',                  (SELECT id FROM crime_categories WHERE name = 'PUBLIC_ORDER'), 2, '90D'),
    ('DISORDERLY_CONDUCT',   (SELECT id FROM crime_categories WHERE name = 'PUBLIC_ORDER'), 1, '90C'),
    ('TRESPASSING',          (SELECT id FROM crime_categories WHERE name = 'PUBLIC_ORDER'), 1, '90J'),
    ('PROSTITUTION',         (SELECT id FROM crime_categories WHERE name = 'PUBLIC_ORDER'), 2, '40A'),
    -- Fraud/Financial
    ('FRAUD',                (SELECT id FROM crime_categories WHERE name = 'FRAUD_FINANCIAL'), 2, '26A'),
    ('FORGERY',              (SELECT id FROM crime_categories WHERE name = 'FRAUD_FINANCIAL'), 2, '250'),
    ('EMBEZZLEMENT',         (SELECT id FROM crime_categories WHERE name = 'FRAUD_FINANCIAL'), 2, '270'),
    ('IDENTITY_THEFT',       (SELECT id FROM crime_categories WHERE name = 'FRAUD_FINANCIAL'), 3, '26E'),
    -- Other
    ('MISSING_PERSON',       (SELECT id FROM crime_categories WHERE name = 'OTHER'), 1, NULL),
    ('NON_CRIMINAL',         (SELECT id FROM crime_categories WHERE name = 'OTHER'), 1, NULL),
    ('UNKNOWN',              (SELECT id FROM crime_categories WHERE name = 'OTHER'), 1, NULL);
