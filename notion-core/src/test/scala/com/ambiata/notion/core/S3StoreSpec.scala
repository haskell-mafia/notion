package com.ambiata.notion.core

class S3StoreSpec extends StoreSpec  {
  def storeType = TemporaryType.S3
  override def is = section("aws") ^ super.is
}
