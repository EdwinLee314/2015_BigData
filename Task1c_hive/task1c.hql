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

DROP TABLE loanquery;
CREATE TABLE loanquery AS
SELECT martotal, round((loancnt * 1.0 / totalcnt) * 100)
FROM (SELECT marital as marloan, count(*) AS loancnt FROM bankinput where loan = '"yes"' GROUP BY marital) hasloan
,(SELECT marital as martotal, count(*) AS totalcnt FROM bankinput GROUP BY marital) totalnum
WHERE marloan = martotal;

INSERT OVERWRITE LOCAL DIRECTORY './out/'
SELECT * FROM loanquery;