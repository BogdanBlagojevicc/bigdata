CREATE TABLE dim_severity (
    id_severity INTEGER NOT NULL,
    name        VARCHAR2(20 CHAR)
);

ALTER TABLE dim_severity ADD CONSTRAINT dim_severity_pk PRIMARY KEY ( id_severity );

CREATE TABLE dim_time_zone (
    id_tz INTEGER NOT NULL,
    name  VARCHAR2(20 CHAR)
);

ALTER TABLE dim_time_zone ADD CONSTRAINT dim_time_zone_pk PRIMARY KEY ( id_tz );

CREATE TABLE dim_tyear (
    id_tyear INTEGER NOT NULL,
    year     VARCHAR2(20 CHAR)
);

ALTER TABLE dim_tyear ADD CONSTRAINT dim_tyear_pk PRIMARY KEY ( id_tyear );

CREATE TABLE dim_type (
    id_type INTEGER NOT NULL,
    name    VARCHAR2(20 CHAR)
);

ALTER TABLE dim_type ADD CONSTRAINT dim_type_pk PRIMARY KEY ( id_type );

CREATE TABLE fact_weather (
    id_weather               INTEGER NOT NULL,
    event_id                 VARCHAR2(50 CHAR),
    precipitation            NUMBER,
    airport_code             VARCHAR2(20 CHAR),
    location_lat             NUMBER,
    location_lng             NUMBER,
    city                     VARCHAR2(50 CHAR),
    country                  VARCHAR2(50 CHAR),
    state                    VARCHAR2(10 CHAR),
    zip_code                 INTEGER,
    start_time               VARCHAR2(50 CHAR),
    end_time                 VARCHAR2(50 CHAR),
    dim_tmonth_id_tmonth     INTEGER NOT NULL,
    dim_severity_id_severity INTEGER NOT NULL,
    dim_type_id_type         INTEGER NOT NULL,
    dim_time_zone_id_tz      INTEGER NOT NULL,
    dim_tyear_id_tyear       INTEGER NOT NULL
);

ALTER TABLE fact_weather ADD CONSTRAINT fact_weather_pk PRIMARY KEY ( id_weather );

ALTER TABLE fact_weather
    ADD CONSTRAINT fact_weather_dim_severity_fk FOREIGN KEY ( dim_severity_id_severity )
        REFERENCES dim_severity ( id_severity );

ALTER TABLE fact_weather
    ADD CONSTRAINT fact_weather_dim_time_zone_fk FOREIGN KEY ( dim_time_zone_id_tz )
        REFERENCES dim_time_zone ( id_tz );

ALTER TABLE fact_weather
    ADD CONSTRAINT fact_weather_dim_tyear_fk FOREIGN KEY ( dim_tyear_id_tyear )
        REFERENCES dim_tyear ( id_tyear );

ALTER TABLE fact_weather
    ADD CONSTRAINT fact_weather_dim_type_fk FOREIGN KEY ( dim_type_id_type )
        REFERENCES dim_type ( id_type );
