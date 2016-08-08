val lines = sc.textFile("/home/cloudera/Downloads/1millionTweets.tsv")
val split_lines = lines.map(_.split("\t"));
val tweets = split_lines.map(record => (record(1), record(2).toInt,record(3)))

val mostCountedTweet = tweets.reduce((x,y) =>(if(x._2 > y._2) x else y ))
val output =  "Month: " + mostCountedTweet._1 + ", count: " + mostCountedTweet._2 + ", hash tag name: " + mostCountedTweet._3;
val writer = new java.io.PrintWriter(new java.io.File("out.txt" ))
writer.write(output)
writer.close()