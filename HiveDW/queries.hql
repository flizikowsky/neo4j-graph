--Total numbers of flights by airline
SELECT
    a.Name AS Airline_Name,
    COUNT(*) AS Total_Flights
FROM Fact_Flight f
JOIN Dim_Airline a
    ON f.Airline_Key = a.Airline_Key
GROUP BY a.Name
ORDER BY Total_Flights DESC;

--Flights with delays > 120 minutes
SELECT
    f.*,
    f.Delay_minutes
FROM Fact_Flight f
WHERE f.Delay_minutes > 120
ORDER BY f.Delay_minutes DESC;

--Number of flights by year and quarter
SELECT
    d.Year,
    d.Quarter,
    COUNT(*) AS Flights
FROM Fact_Flight f
JOIN Dim_Date d
    ON f.Departure_Date_Key = d.Date_Key
GROUP BY d.Year, d.Quarter
ORDER BY d.Year, d.Quarter;

-- Top 10 Most popular airports
SELECT
    ap.Name AS Origin_Airport,
    COUNT(*) AS Flight_Count
FROM Fact_Flight f
JOIN Dim_Airport ap
    ON f.Origin_Airport_Key = ap.Airport_Key
GROUP BY ap.Name
ORDER BY Flight_Count DESC
LIMIT 10;

-- Most popular departure hours
SELECT
    t.hour AS Departure_Hour,
    COUNT(*) AS Flights
FROM Fact_Flight f
JOIN Dim_Time t
    ON f.Departure_Time_Key = t.Time_Key
GROUP BY t.hour
ORDER BY Flights DESC;

-- Average seat utilization
SELECT
    AVG(f.Passenger / a.Capacity) AS Avg_Seat_Utilization
FROM Fact_Flight f
JOIN Dim_Aircraft a
    ON f.Aircraft_Key = a.Aircraft_Key;

-- Most popular routes by passenger count
SELECT
    o.Name AS Origin,
    d.Name AS Destination,
    SUM(f.Passenger) AS Total_Passengers
FROM Fact_Flight f
JOIN Dim_Airport o ON f.Origin_Airport_Key = o.Airport_Key
JOIN Dim_Airport d ON f.Destination_Airport_Key = d.Airport_Key
GROUP BY o.Name, d.Name
ORDER BY Total_Passengers DESC
LIMIT 20;

--Top 10 routes with longest distance
SELECT
    CASE
        WHEN o.Name < d.Name THEN o.Name
        ELSE d.Name
    END AS Location_1,
    CASE
        WHEN o.Name < d.Name THEN d.Name
        ELSE o.Name
    END AS Location_2,
    MAX(f.Distance_km) AS Route_Distance
FROM Fact_Flight f
JOIN Dim_Airport o ON f.Origin_Airport_Key = o.Airport_Key
JOIN Dim_Airport d ON f.Destination_Airport_Key = d.Airport_Key
GROUP BY
    CASE WHEN o.Name < d.Name THEN o.Name ELSE d.Name END,
    CASE WHEN o.Name < d.Name THEN d.Name ELSE o.Name END
ORDER BY Route_Distance DESC
LIMIT 10;

--Routes served by aircraft with Aircraft_Key = 1067
SELECT
    o.Name AS Origin,
    d.Name AS Destination,
    COUNT(*) AS Flights
FROM Fact_Flight f
JOIN Dim_Airport o ON f.Origin_Airport_Key = o.Airport_Key
JOIN Dim_Airport d ON f.Destination_Airport_Key = d.Airport_Key
WHERE f.Aircraft_Key = 1067
GROUP BY o.Name, d.Name
ORDER BY Flights DESC;

--Detection of flights that crossed the equator [complex type usage]
SELECT
    f.Departure_Date_Key,
    f.Arrival_Date_Key,
    a.Model,
    o.Coordinates.lat AS Origin_Lat,
    d.Coordinates.lat AS Dest_Lat
FROM Fact_Flight f
JOIN Dim_Airport o ON f.Origin_Airport_Key = o.Airport_Key
JOIN Dim_Airport d ON f.Destination_Airport_Key = d.Airport_Key
JOIN Dim_aircraft a ON f.Aircraft_Key = a.Aircraft_Key
WHERE (o.Coordinates.lat < 0 AND d.Coordinates.lat > 0)
   OR (o.Coordinates.lat > 0 AND d.Coordinates.lat < 0);

--Aircraft with type Wide-body aircraft [complex type usage]
SELECT
    Aircraft_Key,
    Manufacturer,
    Model,
    Capacity,
    Extra_info
FROM Dim_Aircraft
WHERE Extra_info['type'] = 'Wide-body aircraft';