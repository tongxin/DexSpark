package base

import java.lang.reflect.Method

/**
  * Created by caimy on 2017/1/4.
  */
object DexMethod {
//设置指定类属性值
  def invokeSetMethod(owner:Object, fieldName:String, fieldValue:Object, args:Array[Object]): Unit ={
      val ownerclass = owner.getClass
      val methodName: String = fieldName.substring(0,1).toUpperCase() + fieldName.substring(1)

      var method: Method = null
      method = ownerclass.getMethod("set" + methodName, fieldValue.getClass)
      method.invoke(owner, fieldValue)
    }

//获取指定类属性的值
    def invokeGetMethod(owner:Object, fieldName:String, args:Array[Object]): Object ={
      val ownerclass = owner.getClass
      val methodName:String = fieldName.substring(0,1).toUpperCase()+fieldName.substring(1)

      var method:Method = null
      method = ownerclass.getMethod("get" + methodName)
      method.invoke(owner)
  }
}
