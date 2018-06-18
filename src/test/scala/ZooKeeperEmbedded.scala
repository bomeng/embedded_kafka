/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
