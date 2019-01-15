package com.rczl.entity

case class Play(mac: String, version: String, typecode: String,sp: String,region: String,area: String,
                mediaId:Int, mediaName:String,columnId:Int,columnName:String,contentId:Int,contentName:String,
                showTimeLength:Long,endPlayTime:String,startPlayTime:String,
                playTimeLength:Long, playType:Int,chargeType:Int,month:String
               )
