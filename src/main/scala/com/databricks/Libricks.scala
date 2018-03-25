package com.databricks

object Libricks {
  def supportedApis: List[String] = {
    List(
      "Token API v2.0",
      "Dbfs API v2.0",
      "Libraries API v2.0"
    )
  }
  def main(args: Array[String]): Unit = {
    print(
      """
        |Libricks is a wrapper library for Databricks REST API
        |Supported APIs:
        |---
        |""".stripMargin)
    supportedApis foreach println
  }
}
