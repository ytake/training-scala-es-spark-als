package net.jp.ytake

/**
  * DataFrameのカラム分割
  */
object Extract {

  case class ExtractResult(
                            title: String,
                            release_date: String
                          )

  /**
    * 指定したタイトル表記を title, release_dateに分割
    * @param title タイトル(年)
    * @return
    */
  def year(title: String): ExtractResult = {
    val pattern = "\\(\\d{4}\\)".r
    if (pattern.findFirstMatchIn(title).isEmpty) {
      return ExtractResult(title, "1970")
    }
    val mi = pattern.findFirstMatchIn(title).get
    ExtractResult(
      title.splitAt(mi.start)._1.trim,
      mi.group(0).stripPrefix("(").stripSuffix(")").trim
    )
  }
}
