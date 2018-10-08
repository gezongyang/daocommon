package com.anallslsoftax.stock

import com.anallslsoftax.util.StringUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gzy
  */
object Stock {

  /**
    * 中间表对应的类
    * '核算主体代码' '收款国库代码' '征收机关代码' '预算科目代码' '预算级次' '预算种类' '税款类型' '税目代码' '业务类型' '企业代码' '企业类型' '金额' '账务日期' '所属月份' '调整期标志'
    */
  case class Taxreceipt(s_bookorgcode:String,
                        s_payeetrecode:String,
                        s_taxorgcode:String,
                        s_bdgsbtcode: String,
                        s_bdglevel:String,
                        s_bdgkind:String,
                        s_taxtype:String,
                        s_taxitemcode:String,
                        s_biztype:String,
                        s_entcode:String,
                        s_enttype:String,
                        f_amt:String,
                        d_acct:String,
                        i_ofmonth:String,
                        s_trimflag:String) extends Serializable


  /**
    * 手工税票实体
    * @param I_VOUSRLNO '凭证流水号',
    * @param I_GRPSEQNO '组序号',
    * @param S_BOOKORGDE '核算主体代码',
    * @param D_ACCEPT '受理日期',
    * @param S_TRASRLNO '交易流水号',
    * @param D_ACCT '账务日期',
    * @param I_PARTNO '分区号',
    * @param S_VOUNO '凭证编号',
    * @param D_VOUCHER '凭证日期',
    * @param S_PAYERORGDE '付款人代码',
    * @param S_PAYERACCT '付款人账号',
    * @param S_PAYEROPNBNKNO '付款人开户行行号',
    * @param D_LIMIT '限缴日期',
    * @param S_ETPTYPE '企业类型',
    * @param C_TAXTYPE '税款类型',
    * @param S_TAXITEMDE '税目代码',
    * @param I_TAXNUM '课税数量',
    * @param F_TAXAMT '计税金额',
    * @param F_TAXRATE '税率',
    * @param F_PAYTAXAMT '应缴税额',
    * @param F_DEDUCTAMT '扣除额',
    * @param F_PAYEDTAXAMT '实缴税额',
    * @param C_HANDBOOKKIND '缴款书种类',
    * @param S_PAYEETREDE '收款国库代码',
    * @param S_AIMTREDE '目的国库代码',
    * @param S_TAXORGDE '征收机关代码',
    * @param S_BDGSBTDE '预算科目代码',
    * @param C_BDGLEVEL '预算级次',
    * @param C_BDGKIND '预算种类',
    * @param F_AMT      '金额',
    * @param S_ASTFLAG '辅助标志'
    * @param S_BIZTYPE '业务类型'
    * @param S_TRASTATE '交易状态'
    * @param I_OFMONTH '所属月份'
    * @param C_TRIMFLAG '整理期标志'
    * @param S_BRIEF    '摘要'
    * @param I_CHGNUM   '修改次数'
    * @param S_STATEDEALDE '状态处理码'
    * @param S_INPUTERID '录入员代码'
    * @param S_CHECKERID '复核员代码'
    * @param S_MAINCHECKERID '重要要素复核员代码'
    * @param TS_SYSUP '系统更新时间'
    */
  case class  HvhHndbookDtail(I_VOUSRLNO :Long,
                               I_GRPSEQNO :Long,
                               S_BOOKORGDE :String,
                               D_ACCEPT :String ,
                               S_TRASRLNO :String,
                               D_ACCT :String ,
                               I_PARTNO :Int ,
                               S_VOUNO :String,
                               D_VOUCHER :String,
                               S_PAYERORGDE :String,
                               S_PAYERACCT :String,
                               S_PAYEROPNBNKNO :String,
                               D_LIMIT :String ,
                               S_ETPTYPE :String,
                               C_TAXTYPE :String,
                               S_TAXITEMDE :String,
                               I_TAXNUM :Int,
                               F_TAXAMT :Double,
                               F_TAXRATE :Double,
                               F_PAYTAXAMT :Double,
                               F_DEDUCTAMT :Double,
                               F_PAYEDTAXAMT :Double,
                               C_HANDBOOKKIND :String,
                               S_PAYEETREDE :String,
                               S_AIMTREDE :String,
                               S_TAXORGDE :String,
                               S_BDGSBTDE :String,
                               C_BDGLEVEL :String,
                               C_BDGKIND  :String,
                               F_AMT :Double,
                               S_ASTFLAG :String,
                               S_BIZTYPE :String,
                               S_TRASTATE :String,
                               I_OFMONTH :Int,
                               C_TRIMFLAG :String,
                               S_BRIEF :String,
                               I_CHGNUM :Int,
                               S_STATEDEALDE :String,
                               S_INPUTERID :String,
                               S_CHECKERID :String,
                               S_MAINCHECKERID :String,
                               TS_SYSUP:String
  )



  private def insertHive(spark:HiveContext, tableName: String, dataDF: DataFrame): Unit = {

    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("stock").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)

    //导入隐式转化的包
    import hiveContext.implicits._

    val ttcName = "tp_taxreceipt_collect"
    //企业账户表
    val teName = "td_entinfo"

    //企业账户信息
    val teacName = "tp_entacctinfo"

    //手工税票表
    val htvHandbookDetail = "HTV_HANDBOOK_DETAIL"

    //中间表
    val ttcRDD = sc.textFile("stock/src/main/resources/ttc.txt")


    val teRDD = sc.textFile("")


    val tpecRDD = sc.textFile("")


    val hhdRDD = sc.textFile("stock/src/main/resources/hhd.txt")


    val ttcDS = ttcRDD.map(_.split(",")).map{

    attr =>{

        Taxreceipt( StringUtil.removeSymbol(attr(0), "\""),
                    StringUtil.removeSymbol(attr(1), "\""),
                    StringUtil.removeSymbol(attr(2), "\""),
                    StringUtil.removeSymbol(attr(3), "\""),
                    StringUtil.removeSymbol(attr(4), "\""),
                    StringUtil.removeSymbol(attr(5), "\""),
                    StringUtil.removeSymbol(attr(6), "\""),
                    StringUtil.removeSymbol(attr(7), "\""),
                    StringUtil.removeSymbol(attr(8), "\""),
                    StringUtil.removeSymbol(attr(9), "\""),
                    StringUtil.removeSymbol(attr(10), "\""),
                    StringUtil.removeSymbol(attr(11), "\""),
                    StringUtil.removeSymbol(attr(12), "\""),
                    StringUtil.removeSymbol(attr(13), "\""),
                    StringUtil.removeSymbol(attr(14), "\"")
        )}
    }.toDF()


    insertHive(hiveContext, ttcName, ttcDS)


    hhdRDD.map(_.split(",")).map {

      attr => {





//        HvhHndbookDtail(
//          StringUtil.removeSymbol(attr(0), "\"").toLong,
//          StringUtil.removeSymbol(attr(1), "\"").toLong,
//          StringUtil.removeSymbol(attr(2), "\""),
//          StringUtil.removeSymbol(attr(3), "\""),
//          StringUtil.removeSymbol(attr(4), "\""),
//          StringUtil.removeSymbol(attr(5), "\""),
//          StringUtil.removeSymbol(attr(6), "\""),
//          StringUtil.removeSymbol(attr(7), "\"").toInt,
//          StringUtil.removeSymbol(attr(8), "\""),
//          StringUtil.removeSymbol(attr(9), "\""),
//          StringUtil.removeSymbol(attr(10), "\""),
//          StringUtil.removeSymbol(attr(11), "\""),
//          StringUtil.removeSymbol(attr(12), "\""),
//          StringUtil.removeSymbol(attr(13), "\""),
//          StringUtil.removeSymbol(attr(14), "\""),
//          StringUtil.removeSymbol(attr(15), "\""),
//          StringUtil.removeSymbol(attr(16), "\""),
//          StringUtil.removeSymbol(attr(17), "\"").toInt,
//          StringUtil.removeSymbol(attr(18), "\"").toDouble,
//          StringUtil.removeSymbol(attr(19), "\"").toDouble,
//          StringUtil.removeSymbol(attr(20), "\"").toDouble,
//          StringUtil.removeSymbol(attr(21), "\"").toDouble,
//          StringUtil.removeSymbol(attr(22), "\"").toDouble,
//          StringUtil.removeSymbol(attr(23), "\""),
//          StringUtil.removeSymbol(attr(24), "\""),
//          StringUtil.removeSymbol(attr(25), "\""),
//          StringUtil.removeSymbol(attr(26), "\""),
//          StringUtil.removeSymbol(attr(27), "\""),
//          StringUtil.removeSymbol(attr(28), "\""),
//          StringUtil.removeSymbol(attr(29), "\""),
//          StringUtil.removeSymbol(attr(30), "\"").toDouble,
//          StringUtil.removeSymbol(attr(31), "\""),
//          StringUtil.removeSymbol(attr(32), "\""),
//          StringUtil.removeSymbol(attr(33), "\""),
//          StringUtil.removeSymbol(attr(33), "\"").toInt,
//          StringUtil.removeSymbol(attr(33), "\""),

        
        )

      }
    }


    var ttc = hiveContext.sql("select * from tp_taxreceipt_collect")

    ttc.show()


  }
}
