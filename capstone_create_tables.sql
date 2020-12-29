CREATE TABLE IF NOT EXISTS i94_visit_details_fact (
    "i94rec"              int     NOT NULL     PRIMARY KEY,
    "i94_year"            int,
    "i94_month"           int,
    "i94_citizenship"     varchar,
    "i94_residence"       varchar,
    "i94_port_of_entry"   varchar sortkey,
    "arrival_date"        numeric,
    "arrival_mode"        int,
    "arrival_state"       varchar,
    "departure_date"      numeric,
    "i94_age"             int,
    "travel_purpose"      int,
    "count"               int,
    "birth_year"          int,
    "gender"              varchar,
    "visa_type"           varchar,
    "residence_temp_id"   int,
    "port_temp_id"        int
);


CREATE TABLE IF NOT EXISTS location_codes_dim (
    "location_code_id"      varchar     NOT NULL,
    "country_code"          varchar,
    "country"               varchar,
    "state_code"            varchar,
    "city"                  varchar,
    "state"                 varchar
);


CREATE TABLE IF NOT EXISTS travel_mode_dim (
    "travel_mode_code"      varchar     NOT NULL      PRIMARY KEY,
    "mode"                  varchar
);


CREATE TABLE IF NOT EXISTS travel_purpose_dim (
    "travel_purpose_code"   varchar     NOT NULL      PRIMARY KEY,
    "travel_purpose"        varchar
);


CREATE TABLE IF NOT EXISTS visa_type_codes_dim (
    "visa_code"             varchar     NOT NULL,
    "visa_category"         varchar,
    "visa_travel_purpose"   varchar
);


CREATE TABLE IF NOT EXISTS temperatures_dim (
    "temperature_id"          bigint     NOT NULL   PRIMARY KEY,
    "temp_month"              int,
    "i94_state_code"          varchar,
    "i94_country_code"        varchar,
    "temp_average"            numeric,
    "stat_count"              int,
    "temp_city"               varchar,
    "temp_state"              varchar,
    "temp_country"            varchar
);


CREATE TABLE IF NOT EXISTS state_demographics_dim (
    "i94_state_code"         varchar     NOT NULL     PRIMARY KEY,
    "median_age"             numeric,
    "male_population"        int,
    "female_population"      int,
    "total_population"       int,
    "number_of_veterans"     int,
    "foreign_born"           int,
    "avg_hh_size"            numeric
);


CREATE TABLE IF NOT EXISTS ethnicity_by_state_dim (
    "i94_state_code"          varchar,
    "race"                    varchar,
    "state"                   varchar,
    "count"                   int
);


CREATE TABLE IF NOT EXISTS us_airports_size_dim (
    "i94_port_code"           varchar     NOT NULL     PRIMARY KEY,
    "state_code"              varchar,
    "type"                    varchar,
    "city"                    varchar,
    "ident"                   varchar
);