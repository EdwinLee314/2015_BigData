DROP TABLE twitterinput;
CREATE TABLE twitterinput 
 (hashtag string,
  month string, 
  cnt int, 
  hashtagname string
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH './1millionTweets.tsv'
INTO TABLE twitterinput;

DROP TABLE tweetedmost;
CREATE TABLE tweetedmost AS
SELECT hashtagname, sum(cnt) as total
FROM twitterinput
GROUP BY hashtagname
ORDER BY total DESC
LIMIT 1;

INSERT OVERWRITE LOCAL DIRECTORY './out/'
SELECT * FROM tweetedmost;
