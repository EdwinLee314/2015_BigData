def nextMonth(now: String, next:String): Boolean={
	val nowYear = now.substring(0,4).toInt;
	val nowMonth = now.substring(4,6).toInt;
	val nextYear = next.substring(0,4).toInt;
	val nextMonth = next.substring(4,6).toInt;
	if(nowYear == nextYear){
		if(nextMonth - nowMonth == 1)
		return true
	}
	if(nextYear - nowYear == 1){
		if(nextMonth == 1 && nowMonth == 12)
		return true
	}	
	false
} 

def findMost(set:Iterable[(String, Int)] ): (Int, String, Int, String, Int) = {
	val datas = set.toSeq 
	var most = (0, "", 0, "", 0);
	val size = datas.length
	for( i <- 0 until size){
		for(j <- 0 until size){
			val now = datas(i)
			val next = datas(j)

			if(nextMonth(now._1 , next._1) == true){
				val mount = next._2 - now._2
				if(mount > most._1){
					most = (mount, now._1, now._2, next._1, next._2)
				}
			}
		}
	}
	return most
}

val lines = sc.textFile("/home/cloudera/Downloads/1millionTweets.tsv")
val split_lines = lines.map(_.split("\t"))

val tweets = split_lines.map(record => (record(3) ,(record(1) ,record(2).toInt))).groupByKey()
val tagMore = tweets.filter(_._2.toSeq.length > 1)
val tagSum = tagMore.map(x => (x._1, findMost(x._2))).filter(_._2._1 > 0).sortBy(_._2._1, false).take(1)
val tagMost = tagSum.map(x=> "Hash tag name:" + x._1 + "\n" +"Count of month " + x._2._2 +":" + x._2._3 + "\n" +"Count of month " + x._2._4 +":" + x._2._5+ "\n").mkString("\n")

val writer = new java.io.PrintWriter(new java.io.File("out.txt" ))
writer.write(tagMost)
writer.close()