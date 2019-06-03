//package stackoverflow
package object stackoverflow {
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int

  val appNumPartitions: Int = 20
}
