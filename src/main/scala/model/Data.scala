package model

case class Data(LVLTYP1: Option[Int],
                LVLTYP2: Option[Int],
                ETIME: Option[Int],
                PRESS: Option[Int],
                PFLAG: Option[String],
                GPH: Option[Int],
                ZFLAG: Option[String],
                TEMP: Option[Int],
                TFLAG: Option[String],
                RH: Option[Int],
                DPDP: Option[Int],
                WDIR: Option[Int],
                WSPD: Option[Int],
                override val headerId: Long
               ) extends IgraData