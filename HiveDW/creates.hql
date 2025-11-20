DROP TABLE IF EXISTS Dim_Date;
DROP TABLE IF EXISTS Dim_Airline;
DROP TABLE IF EXISTS Dim_Time;
DROP TABLE IF EXISTS staging_time;
DROP TABLE IF EXISTS staging_date;
DROP TABLE IF EXISTS Dim_Airport;
DROP TABLE IF EXISTS Dim_Aircraft;
DROP TABLE IF EXISTS staging_aircraft;
DROP TABLE IF EXISTS Fact_Flight;
DROP TABLE IF EXISTS staging_flight;
DROP TABLE IF EXISTS staging_airlines;


------------------------------------------------------------------------------------------------------------------------
--Internal table in ORC format with a staging table in delimited text format loaded from HDFS
CREATE TABLE Dim_Date (
    Date_Key DATE NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL
)
COMMENT 'Wymiar daty, zarządzany przez Hive.'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');

CREATE EXTERNAL TABLE staging_date (
    Date_Key STRING NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/user/source/date'
TBLPROPERTIES ("skip.header.line.count"="1");

INSERT OVERWRITE TABLE Dim_Date
SELECT
    TO_DATE(Date_Key),
    Day,
    Month,
    Year,
    Quarter
FROM staging_date;

SELECT * FROM Dim_Date LIMIT 10;
SELECT * FROM staging_date LIMIT 10;



------------------------------------------------------------------------------------------------------------------------
--Internal table in ORC format with a staging table in delimited text format loaded from HDFS
CREATE TABLE Dim_Time (
    Time_Key STRING NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL
)
COMMENT 'Wymiar czasu, zarządzany przez Hive.'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');

CREATE EXTERNAL TABLE staging_time (
    Time_Key string NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/user/source/time'
TBLPROPERTIES ("skip.header.line.count"="1");

INSERT OVERWRITE TABLE Dim_Time SELECT * FROM staging_time;

SELECT * FROM Dim_Time LIMIT 10;
SELECT * FROM staging_time LIMIT 10;



------------------------------------------------------------------------------------------------------------------------
--Internal table in Parquet format with a staging table in delimited text format loaded from HDFS using statics partitioning
CREATE EXTERNAL TABLE staging_airlines (
    Airline_Key STRING,
    Name STRING,
    Country STRING,
    Alliance STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/source/airlines'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM staging_airlines LIMIT 10;


CREATE TABLE Dim_Airline (
    Airline_Key STRING,
    Name STRING,
    Country STRING
    --alliance column removed from here because it is a key for static partitioning
)
COMMENT 'Airline Dimension Static Partitioning'
PARTITIONED BY (Alliance STRING)
STORED AS PARQUET;


SET hive.exec.dynamic.partition=false;


INSERT OVERWRITE TABLE Dim_Airline PARTITION (Alliance='Star Alliance')
SELECT
    Airline_Key,
    Name,
    Country
FROM staging_airlines
WHERE Alliance = 'Star Alliance';

INSERT OVERWRITE TABLE Dim_Airline PARTITION (Alliance='SkyTeam')
SELECT
    Airline_Key,
    Name,
    Country
FROM staging_airlines
WHERE Alliance = 'SkyTeam';

INSERT OVERWRITE TABLE Dim_Airline PARTITION (Alliance='Oneworld')
SELECT
    Airline_Key,
    Name,
    Country
FROM staging_airlines
WHERE Alliance = 'Oneworld';

SELECT * FROM Dim_Airline;


------------------------------------------------------------------------------------------------------------------------
--External table in delimited text format with a CSV file directly loaded from HDFS
CREATE TABLE Dim_Airport (
    Airport_Key STRING,
    Name STRING,
    City STRING,
    Country STRING,
    Coordinates STRUCT<lat: DOUBLE, lon: DOUBLE>
)
COMMENT 'Zewnętrzny wymiar lotnisk, dane źródłowe w CSV.'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    COLLECTION ITEMS TERMINATED BY ':'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/source/airports/airports.csv' OVERWRITE INTO TABLE Dim_Airport;

SELECT * FROM Dim_Airport;


------------------------------------------------------------------------------------------------------------------------
--External table in Parquet format with a staging table in delimited text format loaded from HDFS
CREATE EXTERNAL TABLE Dim_Aircraft (
    Aircraft_Key INT,
    Manufacturer STRING,
    Capacity INT,
    Model STRING,
    Extra_info MAP<STRING, STRING>
)
COMMENT 'Zewnętrzny wymiar samolotów, dane w formacie Parquet.'
STORED AS PARQUET
LOCATION '/data/source/aircraft'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE staging_aircraft (
    Aircraft_Key INT,
    Manufacturer STRING,
    Capacity INT,
    Model STRING,
    Extra_info MAP<STRING, STRING>
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    COLLECTION ITEMS TERMINATED BY '|'
    MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/user/source/aircraft'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM Dim_Aircraft;
SELECT * FROM staging_aircraft;

INSERT OVERWRITE TABLE Dim_Aircraft
SELECT *
FROM staging_aircraft;


------------------------------------------------------------------------------------------------------------------------
CREATE TABLE Fact_Flight (
    Departure_Date_Key DATE,
    Arrival_Date_Key DATE,
    Departure_Time_Key STRING,
    Arrival_Time_Key STRING,
    Origin_Airport_Key STRING,
    Destination_Airport_Key STRING,
    Airline_Key STRING,
    Aircraft_Key INT,
    Departure_time TIMESTAMP,
    Distance_km DECIMAL(10,2),
    Delay_minutes INT,
    Passenger INT
)
COMMENT 'Główna tabela faktów lotów, zarządzana przez Hive.'
PARTITIONED BY (departure_year INT, departure_quarter INT)
CLUSTERED BY (Origin_Airport_Key)
    SORTED BY (Departure_Date_Key) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE EXTERNAL TABLE staging_flight (
    Departure_Date_Key STRING,
    Arrival_Date_Key STRING,
    Departure_Time_Key STRING,
    Arrival_Time_Key STRING,
    Origin_Airport_Key STRING,
    Destination_Airport_Key STRING,
    Airline_Key STRING,
    Aircraft_Key STRING,
    Departure_time STRING,
    Distance_km STRING,
    Delay_minutes STRING,
    Passenger STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/source/flight'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM staging_flight LIMIT 100;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.optimize.sort.dynamic.partition=true;

SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;

INSERT OVERWRITE TABLE Fact_Flight PARTITION (departure_year, departure_quarter)
SELECT
    CAST(Departure_Date_Key AS DATE),
    CAST(Arrival_Date_Key AS DATE),
    Departure_Time_Key,
    Arrival_Time_Key,
    Origin_Airport_Key,
    Destination_Airport_Key,
    Airline_Key,
    CAST(Aircraft_Key AS INT),
    CAST(Departure_time AS TIMESTAMP),
    CAST(Distance_km AS DECIMAL(10,2)),
    CAST(Delay_minutes AS INT),
    CAST(Passenger AS INT),
    YEAR(CAST(Departure_Date_Key AS DATE)) AS departure_year,
    QUARTER(CAST(Departure_Date_Key AS DATE)) AS departure_quarter
FROM staging_flight;

SELECT * FROM Fact_Flight LIMIT 10;