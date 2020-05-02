package com.peng.gmall.realtime.bean

case class StartupLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      logType: String,
                      vs: String,
                      var logDate: String,//日期
                      var logHour: String,//小时
                      ts: Long
                     ) {
}

