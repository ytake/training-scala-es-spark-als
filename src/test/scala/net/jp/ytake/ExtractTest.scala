package net.jp.ytake

import org.junit._
import org.scalatest.junit.AssertionsForJUnit

class ExtractTest extends AssertionsForJUnit {

  @Test
  def testShouldBeExtractTuple(): Unit = {
    val y = Extract.year("Toy Story (1995)")
    Assert.assertEquals("Toy Story", y.title)
    Assert.assertEquals("1995", y.release_date)
  }

  @Test
  def testShouldReturnDefaultYear(): Unit = {
    val y = Extract.year("Jumanji (qwert)")
    Assert.assertEquals("Jumanji (qwert)", y.title)
    Assert.assertEquals("1970", y.release_date)
  }
}
