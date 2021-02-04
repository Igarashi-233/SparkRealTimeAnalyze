case class SortKey(clickCount: Long, OrderCount: Long, payCount: Long) extends Ordered[SortKey] {
  /**
   * this.compare(that)
   * 1)compare>0  this > that
   * 2)compare<0  this < that
   * 3)compare=0  this = that
   */
  override def compare(that: SortKey): Int = {
    if (this.clickCount - that.clickCount != 0) {
      (this.clickCount - that.clickCount).toInt
    } else if (this.OrderCount - that.OrderCount != 0) {
      (this.OrderCount - that.OrderCount).toInt
    } else {
      (this.payCount - that.payCount).toInt
    }
  }

}
