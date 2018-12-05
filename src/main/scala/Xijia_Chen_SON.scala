import java.util.Date
import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import util.control.Breaks._
import org.apache.spark.broadcast.Broadcast


object Xijia_Chen_SON{
  def main(args: Array[String]): Unit = {
    var start_time = new Date().getTime
    val conf = new SparkConf().setAppName("response_count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate

    var raw = sc.textFile(args(0))
    var support = args(1).toInt
    var data = raw.map(row => row.split(",")).map(x=>(x(0), x(1))) //(id, review)

    def buildcandidate(cand: Set[Set[String]], pre: Int, fut: Int): Set[Set[String]] ={ // used to generate candidate for each round
      var singleword = cand.flatten
      var all = singleword.subsets(fut)//all the candidate subset generated from current single candidate word
      var res =  Set.empty[Set[String]]
      for (fut_set<-all){
        var t1 =fut_set.toArray
        var t2 = collection.mutable.ArrayBuffer(t1: _*)
        var allin = true
        breakable{
          for(i<- 0 to fut-1){
            var tmp = t2.clone()
            tmp.remove(i)
            var if_incand = tmp.toSet
            if(cand.contains(if_incand)==false){
              allin = false
              break
            }
          }
        }
        if(allin == true){
          res+=fut_set
        }
        allin = true
      }
      res
    }

    def Mapper_1(par_list: Iterator[Set[String]], sup:Int): Iterator[Set[String]] = {//Apriori algorithm
      var partition = par_list.toList
      var size = 2
      var single = partition.flatten.
        groupBy(identity).mapValues(_.size)
        .filter(x => x._2 >= sup)
        .map(x => x._1).toSet // frequent single items
      var inloop = true
      var cand = single.subsets(size)
      var ret = Set.empty[Set[String]]
      var next_cand = Set.empty[Set[String]]
      var ret_size = ret.size
      while(inloop){
        for (x <- cand){
          var count = 0
          for (each_basket<-partition){if (x.subsetOf(each_basket)){count+=1}}
          if (count>=sup){
            next_cand += x
            ret += x
          }
        }
        if (ret.size >ret_size){
          cand= buildcandidate(next_cand, size, size+1).toIterator
          next_cand = Set.empty[Set[String]]
          size += 1
          ret_size = ret.size
        } else {inloop = false}
      }
      for(i<-single){ret += Set(i.toString())}
      ret.iterator
    }

    def Mapper_2(par_list : List[Set[String]], reduce_1 : Broadcast[Array[Set[String]]]) : Iterator[(Set[String], Int)] = {
      var ret = List[(Set[String], Int)]()
      for (freq_item <- reduce_1.value) {
        var count = 0
        for (long_set <- par_list) {
          if (freq_item.subsetOf(long_set)) {
            count = count + 1
          }
        }
        ret = ret :+ Tuple2(freq_item, count)
      }
      ret.toIterator
    }

     //?????
//    var word_group= data.groupBy(x=>(x._1)).map(x=>{
//      var set = x._2.map(y=>(y._2))
//      (x._1, set.toSet)
//    }).map(x=>x._2)// (id, (review, review))
////    var partition = word_group.partitionBy(new org.apache.spark.HashPartitioner(5))
////    partition.foreach(println)
//    var num_par = word_group.getNumPartitions
//    var f_sup = support/num_par

    var word_group= data.groupByKey().map(x=>x._2.toSet)// (id, (review, review))
    var partition = word_group.repartition(5)
    var num_par = word_group.getNumPartitions
    var f_sup = support/num_par
    var reduce_1 = word_group.mapPartitions(par => {//((word....),(word....))
      Mapper_1(par, f_sup)
    }).map(x=>(x,1))
      .reduceByKey((x,y)=>1)
      .map(_._1).collect() // candidate itemset ->((word), (word,word)....)

    val broad = sc.broadcast(reduce_1)
    var reduce_2 = word_group.mapPartitions(par => {Mapper_2(par.toList, broad)})
      .reduceByKey(_+_).filter(x=>x._2 >= support)
      .map(x=>x._1)
      .map(x => (x.size,x))//[(2,Set(good, say)), (2,Set(next, great))]
//    reduce_2.foreach(println)
    var count = reduce_2.groupByKey().map(x=>(x._1, x._2.size))
    count.foreach(println)

    var max_len = count.max()._1.toInt
    var print_red = reduce_2.collect()
    val writer = new PrintWriter(new File(args(2)))
    for(i <- 1 to max_len){
      var lines = print_red.filter(x=>x._1==i).map(x=>x._2)
      var all = new StringBuilder
      for(word <- lines){
        var word_ = word.mkString("(", ",", ")")+","
        all ++= word_
      }
      var fin = all.toString
      writer.write(fin.substring(0, fin.length()-1))
      writer.write("\n\n")
    }
    writer.close()
    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time)/1000 + " secs")
  }
}
