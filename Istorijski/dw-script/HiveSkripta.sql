CREATE TABLE IF NOT EXISTS dim_severity (
    id_severity INTEGER,
    name        VARCHAR(20),
    CONSTRAINT dim_severity_pk PRIMARY KEY (id_severity) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS dim_time_zone (
    id_tz INTEGER,
    name  VARCHAR(20),
    CONSTRAINT dim_time_zone_pk PRIMARY KEY (id_tz) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS dim_tyear (
    id_tyear INTEGER,
    year     VARCHAR(20),
    CONSTRAINT dim_tyear_pk PRIMARY KEY (id_tyear) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS dim_type (
    id_type INTEGER,
    name    VARCHAR(20),
    CONSTRAINT dim_type_pk PRIMARY KEY (id_type) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS fact_weather (
    id_weather               INTEGER,
    event_id                 VARCHAR(50),
    precipitation            DOUBLE,
    airport_code             VARCHAR(20),
    location_lat             DOUBLE,
    location_lng             DOUBLE,
    city                     VARCHAR(50),
    country                  VARCHAR(50),
    state                    VARCHAR(10),
    zip_code                 INTEGER,
    start_time               VARCHAR(50),
    end_time                 VARCHAR(50),
    dim_severity_id_severity INTEGER,
    dim_type_id_type         INTEGER,
    dim_time_zone_id_tz      INTEGER,
    dim_tyear_id_tyear       INTEGER,
    CONSTRAINT fact_weather_pk PRIMARY KEY (id_weather) DISABLE NOVALIDATE,
    CONSTRAINT fact_weather_dim_time_zone_fk FOREIGN KEY (dim_time_zone_id_tz)
        REFERENCES dim_time_zone (id_tz) DISABLE NOVALIDATE RELY,
    CONSTRAINT fact_weather_dim_tyear_fk FOREIGN KEY (dim_tyear_id_tyear)
        REFERENCES dim_tyear (id_tyear) DISABLE NOVALIDATE RELY,
    CONSTRAINT fact_weather_dim_severity_fk FOREIGN KEY (dim_severity_id_severity)  
        REFERENCES dim_severity (id_severity) DISABLE NOVALIDATE RELY,
    CONSTRAINT fact_weather_dim_type_fk FOREIGN KEY (dim_type_id_type)
        REFERENCES dim_type (id_type) DISABLE NOVALIDATE RELY
);
