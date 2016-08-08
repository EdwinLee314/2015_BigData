val lines = sc.textFile("/home/cloudera/Downloads/docword.txt")
val split_lines = lines.map(_.split(" "));
val words = split_lines.map(record => (record(0).toLong, record(1).toLong, record(2).toLong))

val input = sc.textFile("/home/cloudera/Downloads/vocab.txt").zipWithIndex
val index = input.map(record => ((record._2 + 1).toLong, record._1))

val wordCount = words.map(record => (record._2, record._3) ).reduceByKey(_ + _)
val wordRank = index.join(wordCount).map(x => (x._2._1, x._2._2)).sortBy(_._1)
wordRank.saveAsTextFile("/home/cloudera/Downloads/task3a")