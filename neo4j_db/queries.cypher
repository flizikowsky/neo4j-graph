//Total listening time in seconds


MATCH (u:User)-[:HAS_SESSION]->(s:Session)-[:STREAMED]->(song:Song)
WITH u, sum(song.duration) AS totalListeningTimeSeconds
RETURN u.name AS user, totalListeningTimeSeconds,
       duration({seconds: totalListeningTimeSeconds}) AS totalListeningTime
ORDER BY totalListeningTimeSeconds DESC
LIMIT 3;


//most streamed songs


MATCH (:User)-[:HAS_SESSION]->(s:Session)-[r:STREAMED]->(song:Song)
WHERE s.startTime >= datetime() - duration({days:90})
RETURN song.name AS song, count(r) AS timesStreamed
ORDER BY timesStreamed DESC
LIMIT 10;


//List all sessions for a user


MATCH (u:User {name:'Kuba Flizikowski'})-[:HAS_SESSION]->(s:Session)-[r:STREAMED]->(song:Song)
RETURN u.name AS user, s.sessionId AS session, s.startTime AS start, s.endTime AS end,
       song.name AS song, r.order AS playOrder
ORDER BY s.startTime, session, r.order;


//amount of listeners for an artist and amount of unique ones


MATCH (u:User)-[:HAS_SESSION]->(:Session)-[st:STREAMED]->(song:Song)
MATCH (song)-[:ON_ALBUM]->(:Album)<-[:RELEASED]-(ar:Artist)
WHERE st.last_played >= datetime() - duration('P30D')
RETURN ar.name AS artist,
count(*) AS totalStreams,
count(DISTINCT u) AS uniqueListeners
ORDER BY totalStreams DESC, uniqueListeners DESC;


//user-based song recommendation (more than direct friends)


MATCH (u:User {name: 'Kuba Flizikowski'})-[:IS_FOLLOWING*1..2]->(friend:User)
WHERE u <> friend
MATCH (friend)-[:HAS_SESSION]->(:Session)-[r:STREAMED]->(song:Song)
WHERE r.last_played >= datetime() - duration({days:30})
  AND NOT (u)-[:HAS_SESSION]->(:Session)-[:STREAMED]->(song)
WITH song, count(r) AS friendStreamCount
ORDER BY friendStreamCount DESC
LIMIT 2
RETURN song.name AS recommendedSong, friendStreamCount AS timesStreamedByFriends;