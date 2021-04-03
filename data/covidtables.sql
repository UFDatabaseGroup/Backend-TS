create table USERS (
    username varchar(12) not null,
    password varchar(12) not null,
    primary key (username)
);

create table COVID_DATA (
    deaths number,
    confirmed number,
    active number,
    recovered number,
    incidence number,
    case_fatality_ratio number,
    admin2 varchar(64),
    state varchar(64),
    country varchar(64) not null,
    timestamp_id number not null,
    timestamp date not null,
    latitude number,
    longitude number
    -- add constraints later
    -- constraint timestamp_fk foreign key (timestamp_id) references timestamp(timestamp_id),
    -- constraint COUNTRY_FK foreign key ("COUNTRY") references "COUNTRY"("NAME"),
);
create index covid_data_timestamp_index on COVID_DATA(timestamp_id);
create index covid_data_country_index on COVID_DATA(country);

create table TIMESTAMP (
    timestamp_id number not null,
    year numeric(4),
    month numeric(2),
    day numeric(2),
    primary key (timestamp_id)
);

create table COUNTRY (
    name varchar(64) not null,
    population number,
    yearly_change number,
    net_change number,
    density number,
    land_area number,
    migrants number, 
    median_age number,
    urban_population number,
    primary key (name)
);

create table UNEMPLOYMENT (
    unemployment_time_stamp date,
    value number,
    country_name varchar(64)
    -- constraint country_fk foreign key (country_name) references COUNTRY(name),
);
create index unemployment_country_index on UNEMPLOYMENT(country_name);

alter table COVID_DATA add constraint timestamp_fk foreign key (timestamp_id) references TIMESTAMP(timestamp_id);
alter table COVID_DATA add constraint country_fk foreign key (country) references COUNTRY(name);
