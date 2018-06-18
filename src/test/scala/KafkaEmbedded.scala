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

import java.io.{File, IOException}
import java.util.Properties

import kafka.admin.RackAwareMode
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.rules.TemporaryFolder
import org.slf4j.{Logger, LoggerFactory}

class KafkaEmbedded(config: Properties) {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  // some default settings
  private val DEFAULT_ZK_CONNECT = "127.0.0.1:2181"
  private val DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000
  private val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000

  private val tmpFolder: TemporaryFolder = new TemporaryFolder()
  tmpFolder.create()
  private val logDir: File = tmpFolder.newFolder()
  private val effectiveConfig: Properties = effectiveConfigFrom(config)
  val kafkaConfig: KafkaConfig = new KafkaConfig(effectiveConfig, true)
  log.debug(s"Starting embedded Kafka broker (with log.dirs=$logDir and ZK ensemble at $zookeeperConnect) ...")
  val kafka: KafkaServer = TestUtils.createServer(kafkaConfig, Time.SYSTEM)
  log.debug(s"Startup of embedded Kafka broker at $brokerList completed (with ZK ensemble at $zookeeperConnect) ...")

  def zookeeperConnect: String = effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT)

  def brokerList: String = String.join(":",
    kafka.config.hostName, Integer.toString(kafka.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))))

  // stop the kafka and clean up the temp folder
  def stop(): Unit = {
    log.debug(s"Shutting down embedded Kafka broker at $brokerList (with ZK ensemble at $zookeeperConnect) ...")
    kafka.shutdown
    kafka.awaitShutdown
    log.debug(s"Removing temp folder $tmpFolder with logs.dir at $logDir ...")
    tmpFolder.delete()
    log.debug(s"Shutdown of embedded Kafka broker at $brokerList completed (with ZK ensemble at $zookeeperConnect) ...")
  }

  // create a topic
  def createTopic(topic: String): Unit = {
    createTopic(topic, 1, 1, new Properties())
  }

  // create a topic
  def createTopic(topic: String, partitions: Int, replication: Int): Unit = {
    createTopic(topic, partitions, replication, new Properties())
  }

  // create a topic
  def createTopic(topic: String, partitions: Int, replication: Int, topicConfig: Properties): Unit = {
    log.debug(s"Creating topic { name: $topic, partitions: $partitions, replication: $replication, config: $topicConfig }")
    val kafkaZkClient = createZkClient
    val adminZkClient: AdminZkClient = new AdminZkClient(kafkaZkClient)
    adminZkClient.createTopic(topic, partitions, replication, topicConfig, RackAwareMode.Enforced)
    kafkaZkClient.close()
  }

  // delete the topic
  def deleteTopic(topic: String): Unit = {
    log.debug(s"Deleting topic { name: {$topic} }")
    val kafkaZkClient = createZkClient
    val adminZkClient = new AdminZkClient(kafkaZkClient)
    adminZkClient.deleteTopic(topic)
    kafkaZkClient.close()
  }

  // create zookeeper client
  private def createZkClient = KafkaZkClient.apply(zookeeperConnect, isSecure = false, DEFAULT_ZK_SESSION_TIMEOUT_MS,
    DEFAULT_ZK_CONNECTION_TIMEOUT_MS, Integer.MAX_VALUE, Time.SYSTEM, "testMetricGroup", "testMetricType")

  @throws[IOException]
  private def effectiveConfigFrom(initialConfig: Properties) = {
    val effectiveConfig = new Properties()
    effectiveConfig.put(KafkaConfig.BrokerIdProp, String.valueOf(0))
    effectiveConfig.put(KafkaConfig.HostNameProp, "127.0.0.1")
    effectiveConfig.put(KafkaConfig.PortProp, "9092")
    effectiveConfig.put(KafkaConfig.NumPartitionsProp, String.valueOf(1))
    effectiveConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, String.valueOf(true))
    effectiveConfig.put(KafkaConfig.MessageMaxBytesProp, String.valueOf(1000000))
    effectiveConfig.put(KafkaConfig.ControlledShutdownEnableProp, String.valueOf(true))
    effectiveConfig.putAll(initialConfig)
    effectiveConfig.setProperty(KafkaConfig.LogDirProp, logDir.getAbsolutePath)
    effectiveConfig
  }
}

object KafkaEmbedded {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val DEFAULT_BROKER_PORT = String.valueOf(0)

  def main(args: Array[String]): Unit = {
    log.debug("Initiating embedded Kafka cluster startup")
    log.debug("Starting a ZooKeeper instance...")
    val zookeeper = new ZooKeeperEmbedded()
    log.debug(s"ZooKeeper instance is running at ${zookeeper.connectString}")

    val brokerConfig: Properties = new Properties()
    val effectiveBrokerConfig: Properties = effectiveBrokerConfigFrom(brokerConfig, zookeeper)
    val port = effectiveBrokerConfig.getProperty(KafkaConfig.PortProp)
    log.debug(s"Starting a Kafka instance on port $port ...")
    val broker = new KafkaEmbedded(effectiveBrokerConfig)
    broker.createTopic("Test")

    Thread.sleep(5 * 1000)

    broker.deleteTopic("Test")
    broker.stop()
    zookeeper.stop()
  }

  private def effectiveBrokerConfigFrom(brokerConfig: Properties, zookeeper: ZooKeeperEmbedded) = {
    val effectiveConfig = new Properties()
    effectiveConfig.putAll(brokerConfig)
    effectiveConfig.put(KafkaConfig.ZkConnectProp, zookeeper.connectString)
    effectiveConfig.put(KafkaConfig.PortProp, DEFAULT_BROKER_PORT)
    effectiveConfig.put(KafkaConfig.DeleteTopicEnableProp, String.valueOf(true))
    effectiveConfig.put(KafkaConfig.LogCleanerDedupeBufferSizeProp, String.valueOf(2 * 1024 * 1024L))

    effectiveConfig
  }
}