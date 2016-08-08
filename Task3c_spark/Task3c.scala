val task3cIn = sc.objectFile[(String,String)]("/home/cloudera/Downloads/InvertedIndex")
val task3cWord = task3cIn.filter(_._1 equals("car")).collect