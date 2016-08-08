val lines = sc.textFile("/home/cloudera/Downloads/1millionTweets.tsv")
val split_lines = lines.map(_.split("\t"));
val mostTweeted = split_lines.map(record => (record(3), record(2).toInt)).reduceByKey(_ + _).sortBy(_._2, false).take(1)
val writer = new java.io.PrintWriter(new java.io.File("out.txt" ))
writer.write(mostTweeted.toString)
writer.close()