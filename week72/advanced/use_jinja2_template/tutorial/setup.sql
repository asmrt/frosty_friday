--!jinja

CREATE SCHEMA {{env}};

CREATE TABLE RAW (COL OBJECT)
    DATA_RETENTION_TIME_IN_DAYS = {{retention_time}};
