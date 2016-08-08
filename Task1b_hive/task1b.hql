DROP TABLE bankinput;
CREATE TABLE bankinput 
 (age int,
  job string, 
  marital string, 
  education string, 
  credit string, 
  balance int,
  housing string,
  loan string,
  contact string,
  day int,
  month string,
  duration int,
  campaign int,
  pdays int,
  previous int,
  poutcome string,
  termdeposit string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH './bank-full.csv'
INTO TABLE bankinput;

DROP TABLE educationquery;
CREATE TABLE educationquery AS
SELECT education , round(avg(balance))
FROM bankinput
GROUP BY education;

INSERT OVERWRITE LOCAL DIRECTORY './task1b-out/'
SELECT * FROM educationquery;