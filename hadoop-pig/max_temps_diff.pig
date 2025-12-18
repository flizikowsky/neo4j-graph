set pig.temp.dir /user/andb34/pigtmp
recs = LOAD '/user/andb34/austin_daily_weather.tsv'
       USING PigStorage('\t')
       AS (
           c1:chararray,
           c2:chararray,
           date:chararray,
           tmax:double,
           c5:chararray,
           tmin:double
       );
recs2 = FOREACH recs GENERATE SUBSTRING(date, 0, 4) AS year, tmax, tmin;
recs3 = GROUP recs2 BY year;
recs4 = FOREACH recs3 GENERATE group AS year, MAX(recs2.tmax) AS max_temp, MIN(recs2.tmin) AS min_temp;
recs5 = FOREACH recs4 GENERATE year, (max_temp - min_temp) AS temp_diff;
recs6 = ORDER recs5 BY temp_diff DESC;
recs7 = LIMIT recs6 1;
DUMP recs7;