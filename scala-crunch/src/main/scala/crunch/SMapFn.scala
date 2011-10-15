package crunch

import com.cloudera.crunch.MapFn;

class SMapFn[S, T](fn: Any => T) extends MapFn[S, T] {
  override def map(input: S): T = {
    Conversions.s2c(fn(Conversions.c2s(input))).asInstanceOf[T]
  }
}
