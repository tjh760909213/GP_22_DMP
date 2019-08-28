import com.hbasepool
import org.junit.Test

class TestHbase {
  @Test
  def testOne(): Unit = {
    println ( hbasepool.conn )

  }

}
