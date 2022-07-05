package model

case class Header(
                    HEADREC : Option[String],
                    ID  : Option[String],
                    YEAR : Option[String],
                    MONTH : Option[String],
                    DAY : Option[String],
                    HOUR : Option[Int],
                    RELTIME : Option[Int],
                    NUMLEV : Option[Int],
                    P_SRC : Option[String],
                    NP_SRC : Option[String],
                    LAT : Option[Int],
                    LON : Option[Int],
                    override val headerId: Long
                 ) extends IgraData




