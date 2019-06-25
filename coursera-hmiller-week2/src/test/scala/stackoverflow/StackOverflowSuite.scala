package stackoverflow

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.nio.file.Paths

import StackOverflow._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  trait TestSuite {
    val spark = SparkSession.builder().appName("Test-StackOverflowSuite").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val lines = sc.textFile(Paths.get(getClass().
                    getResource("/stackoverflow/stackoverflow.csv").toURI).
                    toString, 20).sample(false, 0.3)

    val raw = rawPostings(lines)

    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped).persist()
    val vectors = vectorPostings(scored).persist()

    val oldcentroids: Array[(Int, Int)] = sampleVectors(vectors)

    //val vectorsSample = vectors.take()
  }

  test ("Test that the Grouped Function Returns 1 (Question, Answer) pair for QID==13730490") {
    new TestSuite {
      val QandAofQID_13730490 = grouped.filter((a) => !(a._2.filter(b => (b._2.parentId.getOrElse(0) == 13730490)).isEmpty)).collect().toList
      assert(QandAofQID_13730490.map({ case ((a: QID, b: Iterable[(Question, Answer)])) => b.size }) === List(1))
    }
  }

  test ("Test that the Grouped Function Returns 4 (Question, Answer) pairs for QID==12590505") {
    new TestSuite {
      // 1 QUESTION of ID 12590505, with 4 ANSWERS of ID 12590505 - so 4 Pairs of (QUESTION, ANSWER)
      val QandAofQID_12590505 = grouped.filter((a) => !(a._2.filter(b => (b._2.parentId.getOrElse(0)==12590505)).isEmpty)).collect().toList
      assert(QandAofQID_12590505(0)._2.size == 4)
      //assert(QandAofQID_12590505.map({case ((a:QID, b:Iterable[(Question, Answer)])) => b.size}) === List(4))
    }
  }

  test ("Test that the Scored Function returns Highest Scored Answer of 6, for the QID==12590505") {
    new TestSuite {
      //assert(scored.filter((a) => (a._1.id == 12590505)).collect().toList.map({case (a:Question,b:Int) => b}) === 6)
      val result = scored.filter((a) => (a._1.id == 12590505)).collect().toList
      assert(result.length == 1)
      assert(result.maxBy(_._2)._2 == 6)
    }
  }

  test ("Test that the Scored Function returns Highest Scored Answer of 6, for the QID==1275960") {
    new TestSuite {
      //assert(scored.filter((a) => (a._1.id == 12590505)).collect().toList.map({case (a:Question,b:Int) => b}) === 6)
      val result = scored.filter((a) => (a._1.id == 1275960)).collect().toList
      assert(result.length == 1)
      assert(result.maxBy(_._2)._2 == 5)
      assert(result(0)._2 == 5)
      assert(result(0)._1.isInstanceOf[Posting])
      assert(result(0)._2.isInstanceOf[HighScore])
    }
  }
  // FILTERED RDD from above Filter ops returns structures that look like below
  /*(13730490,CompactBuffer((Posting(1,13730490,None,None,1,Some(Ruby)),
                            Posting(2,13730567,None,Some(13730490),5,None)
                          )))*/

  test("Test that 'scored' RDD's size is 2121822") {
    new TestSuite {
      assert(scored.collect().toList.length === 2121822)
    }
  }

  test("Verify that sampleVectors method returns an Array[(Int, Int)] of size.m == kMeansCluster") {
    new TestSuite {
      assert(oldcentroids.size === kmeansKernels, "Size of Sample Vectors output must be equal to # of KMeansKernels")
    }
  }

  test ("Test that NewMeans Function returns an Array[(Int, Int] of size that is less than or equal to old centroid size") {
    new TestSuite {
      val newcentroids = computeNewCentroids(oldcentroids, vectors)
      assert(newcentroids.length <= oldcentroids.length, "New Centroid Length must be Equal to Old Centroid Length")
    }
  }

  test ("test that the calcMedian function works on Odd and Even Lists") {
    val listtobeTested = List(1,3,5,7,8,10,22,34,44,55)
    assert(calcMedian(listtobeTested) == 9)

    val listtobeTesttedOdd = List(1,3,5,7,8,10,22,34,44,55,56)
    assert(calcMedian(listtobeTesttedOdd)==10)
  }

}
