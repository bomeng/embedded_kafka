import org.apache.curator.test.TestingServer
import org.slf4j.{Logger, LoggerFactory}

class ZooKeeperEmbedded {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  log.debug("Starting embedded ZooKeeper server...")

  private val server: TestingServer = new TestingServer()

  log.debug(s"Embedded ZooKeeper server at ${server.getConnectString} uses the temp directory at ${server.getTempDirectory}")

  def stop(): Unit = {
    log.debug(s"Shutting down embedded ZooKeeper server at ${server.getConnectString} ...")
    server.close()
    log.debug(s"Shutdown of embedded ZooKeeper server at ${server.getConnectString} completed")
  }

  val connectString: String = server.getConnectString

  val hostname: String = connectString.substring(0, connectString.lastIndexOf(':'))
}

object ZooKeeperEmbedded {
  def main(args: Array[String]): Unit = {
    val zookeeper = new ZooKeeperEmbedded()

    // sleep for 5 seconds
    Thread.sleep(5 * 1000)

    zookeeper.stop()
  }
}
