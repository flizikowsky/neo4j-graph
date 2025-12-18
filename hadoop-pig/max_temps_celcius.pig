set pig.temp.dir /user/andb34/pigtmp
recs = load '/user/andb34/austin_daily_weather.tsv';
recs2 = FOREACH recs GENERATE $2, $3;
recs3 = FOREACH recs2 GENERATE SUBSTRING($0,0,4), $1;
recs4 = GROUP recs3 BY $0;
recs5 = FOREACH recs4 GENERATE group, MAX(recs3.$1);
recs6 = FOREACH recs5 GENERATE $0, ROUND(($1 - 32.0) * (5.0 / 9.0) * 100.0) / 100.0;
dump recs6;