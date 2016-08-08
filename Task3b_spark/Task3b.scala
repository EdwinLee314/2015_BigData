val lines = sc.textFile("/home/cloudera/Downloads/docword.txt")
val split_lines = lines.map(_.split(" "));
val words = split_lines.map(record => (record(0).toLong, record(1).toLong, record(2).toLong))

val input = sc.textFile("/home/cloudera/Downloads/vocab.txt").zipWithIndex
val index = input.map(record => ((record._2 + 1).toLong, record._1))

val wordInvert =  words.map(x => (x._2,(x._1,x._3)) ).groupByKey().map(x => (x._1,x._2.toSeq.sortWith(_._2 > _._2).mkString))
val task3b = index.join(wordInvert).map(x => x._2).sortBy(_._1)
val task3b2 = task3b.map(x => x._1 + " " + x._2)
task3b2.saveAsTextFile("/home/cloudera/Downloads/task3b")
task3b.saveAsObjectFile("/home/cloudera/Downloads/InvertedIndex")