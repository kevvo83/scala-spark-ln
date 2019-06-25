package stackoverflow

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._

import scala.annotation.tailrec
import java.lang.String
import java.lang.Integer


/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  //@transient lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StackOverflow")
  //@transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().
      appName("StackOverflow").
      master(Option(System.getenv("SPARKMASTER")).getOrElse("local[*]")).
      enableHiveSupport().config("spark.sql.warehouse.dir", Option(System.getenv("WAREHOUSEDIR")).getOrElse("")).
      getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //val ipFileDefault: String = Paths.get(getClass().getResource("/stackoverflow/stackoverflow.csv").toURI).toString
    val ipFile: String = Option(args(0)).getOrElse("")

    val lines = sc.textFile(ipFile, 20)

    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored).persist(MEMORY_ONLY)

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }

}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable with StackOverflowTrait {

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings filter (_.postingType == 1)
    val answers = postings filter (_.postingType == 2)

    val qs_proc: RDD[(QID, Question)] = questions map (a=> (a.id, a))
    val ans_proc: RDD[(QID, Answer)] = answers map (b=> (b.parentId.getOrElse(0), b))

    val rp1 = new RangePartitioner(20, qs_proc)
    val qs_proc_part = qs_proc.partitionBy(rp1)

    val rp2 = new RangePartitioner(20, ans_proc)
    val ans_proc_part = ans_proc.partitionBy(rp1)

    ((qs_proc join ans_proc) groupByKey())
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    grouped.mapPartitions[(Question, HighScore)](it => {

      def answerHighScore(as: Array[Answer]): HighScore = {
        var highScore = 0
        var i = 0
        while (i < as.length) {
          val score = as(i).score
          if (score > highScore)
            highScore = score
          i += 1
        }
        highScore
      }

      var result: List[(Question, HighScore)] = List()

      while (it.hasNext) {
        val elem = it.next()
        val question: Question = elem._2.head._1
        val arrayOfAnswersForThisQID: Array[Answer] = elem._2.unzip._2.toArray
        val highScoreAnswer:HighScore = answerHighScore(arrayOfAnswersForThisQID)

        result = result :+ (question, highScoreAnswer)
      }

      result.toIterator
    })

    // compare the performance using the REDUCE below, versus the def ANSWERHIGHSCORE above

    // Maybe pair.toArray.unzip._2 takes too much memory, as compared to the pair.map(_._2)
    /*grouped.mapValues(pair => {
      val listofAnswers: Array[Answer] = pair.toArray.unzip._2
      val q: Question = pair.toArray.unzip._1(0)
      (q, answerHighScore(listofAnswers))
    }).map({case (qid: QID,(question: Question, hiscore: HighScore))=> (question, hiscore)} )*/

    // Previous submission
    //grouped.map({case (qid:QID, vals:Iterable[(Question, Answer)]) => (vals.head._1, answerHighScore(vals.map(_._2).toArray))})

    // Look into reduceByKey
    /*val qid_vs_highestScoredAns: RDD[(QID, (Question, Answer))] =
      (grouped.mapValues((e:Iterable[(Question, Answer)]) => e reduce[(Question, Answer)]
        ((l:(Question, Answer), r:(Question, Answer)) =>
          if (l._2.score > r._2.score) l else r)))

    qid_vs_highestScoredAns.map({case (a:QID, b:(Question, Answer)) => (b._1, b._2.score)})*/

  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {

    val res = scored map (ip => {

      /** Return optional index of first language that occurs in `tags`. */
      def firstLangInTag (tag: Option[String], ls: List[String]): Option[Int] = {

        if (tag.isEmpty) None
        else if (ls.isEmpty) None
        else if (tag.get == ls.head) Some(0) // index: 0
        else {
          val tmp = firstLangInTag(tag, ls.tail)
          tmp match {
            case None => None
            case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
          }
        }
      }

      ip match {
        case (a:Question, hi:HighScore) => (firstLangInTag(a.tags, langs).getOrElse(1) * langSpread,hi)
      }

    })

    scored mapPartitions[(LangIndex, HighScore)](it => {

      /** Return optional index of first language that occurs in `tags`. */
      def firstLangInTag (tag: Option[String], ls: List[String]): Option[Int] = {

        if (tag.isEmpty) None
        else if (ls.isEmpty) None
        else if (tag.get == ls.head) Some(0) // index: 0
        else {
          val tmp = firstLangInTag(tag, ls.tail)
          tmp match {
            case None => None
            case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
          }
        }
      }

      var mapres: List[(LangIndex, HighScore)] = List()

      while (it.hasNext) {
        val elem = it.next()
        mapres :+ (firstLangInTag(elem._1.tags, langs).getOrElse(1) * langSpread, elem._2)
      }

      mapres.toIterator
    })

    val rp = new RangePartitioner(10, res)
    res.partitionBy(rp)//.persist(MEMORY_ONLY)

  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  def computeNewCentroids(oldcentroids: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(Int, (Int, Int))] = {

    // 2. For each Vector in Vectors, determine to which Cluster each point belongs to
    // This is done by meas
    val rdd_newcentroid_vs_vector: RDD[(Int, (Int, Int))] = for {
      vector <- vectors
    } yield (findClosest(vector, oldcentroids), vector)

    // 3. Group the above Structure by Cluster
    // 3.1 Then find the mean of each new Cluster - these will be the new Centroids
    rdd_newcentroid_vs_vector.groupByKey().mapValues(a=> averageVectors(a)) collect()

  }


  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    // Step 1. - centroids are passed in - DONE
    var newMeans = means.clone() // you need to compute newMeans

    // Steps 2. and 3.
    val newMeans_t = computeNewCentroids(means, vectors)

    // Step 4. - update the newMeans structure with the newly computed centroids
    for ((idx, values) <- newMeans_t) newMeans.update(idx, values)

    assert(means.length == newMeans.length, "Means and New Means should be of the same length")

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  /** Helper function to compute Median */
  def calcMedian(in: List[Int]): Int = {
    val tmp:List[Int] = in sortWith ((a,b) => (a<b))
    val length: Int = tmp.length
    if (length % 2 == 0) { (tmp(length/2 - 1) + tmp(length/2)) /2 }
    else tmp(length/2)
  }

  /** Function to Cluster Vectors together and summarize the Results */
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {

    val closest = vectors.map(p => (findClosest(p, means), p)) // (centroididx, (langidx, hiscore))
    val closestGrouped = closest.groupByKey() // (centroididx, Iterable[(langidx, hiscore)])

    val median = closestGrouped.mapValues { vs => // Iterable[(langidx, hiscore)]

      val langidx_vs_numquestions:Map[Int, Int] = vs.map(_._1 / langSpread).groupBy(identity).mapValues(_.size)

      val totalNumberOfQuestionsInCluster: Int = vs.size // langidx_vs_numquestions.reduce((a:(Int, Int),b:(Int, Int))=> a._2 + b._2)
      val tmp: Int = langidx_vs_numquestions.toList.unzip._2.sum

      val langLabel: String   = langs(langidx_vs_numquestions.maxBy(_._2)._1) // most common language in the cluster

      val langPercent: Double = (langidx_vs_numquestions.maxBy(_._2)._2 / totalNumberOfQuestionsInCluster) * 100 // percent of the questions in the most common language
      val clusterSize: Int    = totalNumberOfQuestionsInCluster

      val medianScore: Int = calcMedian(vs.map(_._2).toList.sorted)

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
