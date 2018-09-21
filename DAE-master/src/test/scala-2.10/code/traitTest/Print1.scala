package code.traitTest

/**
  * Created by asus on 2017/3/23.
  */
trait Print1 extends BaseClass{
  override def show(): Unit = {
    print(a+" from Print1")
  }
}
