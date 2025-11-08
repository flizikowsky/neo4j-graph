DROP TABLE IF EXISTS flight_fact;
DROP TABLE IF EXISTS staging_flight_fact;


-----Flight Fact table-----

CREATE TABLE IF NOT EXISTS staging_flight_fact (
    Date_Key                STRING,
    Arrival_Date_Key        STRING,
    Origin_Airport_Key      INT,
    Destination_Airport_Key INT,
    Airline_Key             INT,
    Aircraft_Key            INT,
    Flight_Duration         INT,
    Departure_time          STRING,
    Arrival_time            STRING,
    Distance_km             DOUBLE,
    Delay_minutes           INT,
    Passengers              INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

//load csv data into the staging table for it to be used in the next step
//TODO: data for flight_fact is not available yet
LOAD DATA INPATH '/user/hive/raw/flights.csv'
INTO TABLE staging_flight_fact;

//to allow for dynamic partitioning and bucketing
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing = true;


CREATE TABLE IF NOT EXISTS flight_fact (
    Date_Key                DATE,
    Arrival_Date_Key        DATE,
    Origin_Airport_Key      INT,
    Destination_Airport_Key INT,
    Airline_Key             INT,
    Aircraft_Key            INT,
    Flight_Duration         INT,
    Departure_time          TIMESTAMP,
    Arrival_time            TIMESTAMP,
    Distance_km             DOUBLE,
    Delay_minutes           INT,
    Passengers              INT
)
PARTITIONED BY (
    year INT,
    month INT
)
CLUSTERED BY (Airline_Key) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='ZLIB'
);


//insert data from the staging table into the final table
INSERT INTO TABLE flight_fact
PARTITION (year, month)
SELECT
    CAST(Date_Key AS DATE)                AS Date_Key,
    CAST(Arrival_Date_Key AS DATE)        AS Arrival_Date_Key,
    Origin_Airport_Key,
    Destination_Airport_Key,
    Airline_Key,
    Aircraft_Key,
    Flight_Duration,
    CAST(Departure_time AS TIMESTAMP)     AS Departure_time,
    CAST(Arrival_time AS TIMESTAMP)       AS Arrival_time,
    Distance_km,
    Delay_minutes,
    Passengers,
    YEAR(CAST(Date_Key AS DATE))          AS year,
    MONTH(CAST(Date_Key AS DATE))         AS month
FROM staging_flight_fact;


-----Airline Dimensional Table-----
CREATE TABLE IF NOT EXISTS dim_airline (
    Airline_Key   INT,
    Airline_Code  STRING,
    Airline_Name  STRING,
    Country       STRING,
    Alliance      STRING,
    Hub_Airports  ARRAY<STRING>,
    Active_Flag   BOOLEAN
)
CLUSTERED BY (Airline_Key) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);



