package com.anallslsoftax.conf

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}


object ConfigurationManager {

  private val params = new Parameters()


  //创建加载类的实例对象
  private val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
                            .configure(params.properties().setFileName("anallslsoftax.properties"))

  //获取配置对象
  val config = builder.getConfiguration()


}
