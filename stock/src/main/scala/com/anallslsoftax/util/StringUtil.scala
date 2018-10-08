package com.anallslsoftax.util

import com.mysql.jdbc.StringUtils

/**
  * Created by gzy
  */
object StringUtil {

  def removeSymbol(field: String, sign: String) = {

    if (StringUtils.isNullOrEmpty(field)) {
      field
    } else {

      field.replaceAll(sign, "")
    }

  }
}
