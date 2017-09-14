package base

import scala.beans.BeanProperty

/**
  * Created by caimy on 2017/1/6.
  * 数据库连接类
  * parameters:
  * url: 数据库连接url
  * user: 数据库用户
  * passwd: 用户登陆密码
  * driver: 数据库连接驱动
  */
class DBConnProperties (@BeanProperty var url:String,
                        @BeanProperty var user:String,
                        @BeanProperty var passwd:String,
                        @BeanProperty var driver:String)
