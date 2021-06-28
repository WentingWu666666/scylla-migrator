package com.scylladb.migrator

import com.datastax.spark.connector.cql.CassandraConnectorConf._
import com.datastax.spark.connector.cql._
import com.scylladb.migrator.config.{ Credentials, SourceSettings, TargetSettings }
import org.apache.spark.SparkConf

import java.net.InetSocketAddress

object Connectors {
  def sourceConnector(sparkConf: SparkConf, sourceSettings: SourceSettings.Cassandra) =
    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts = Set(new InetSocketAddress(sourceSettings.host, sourceSettings.port)),
          authConf = sourceSettings.credentials match {
            case None                                  => NoAuthConf
            case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
          },
          cassandraSSLConf = getCassandraSSLConfFromSparkConf(sparkConf)
        ),
        localConnectionsPerExecutor  = sourceSettings.connections,
        remoteConnectionsPerExecutor = sourceSettings.connections,
        queryRetryCount              = -1
      )
    )

  def targetConnector(sparkConf: SparkConf, targetSettings: TargetSettings.Scylla) =
    new CassandraConnector(
      CassandraConnectorConf(sparkConf).copy(
        contactInfo = IpBasedContactInfo(
          hosts = Set(new InetSocketAddress(targetSettings.host, targetSettings.port)),
          authConf = targetSettings.credentials match {
            case None                                  => NoAuthConf
            case Some(Credentials(username, password)) => PasswordAuthConf(username, password)
          },
          cassandraSSLConf = getCassandraSSLConfFromSparkConf(sparkConf)
        ),
        localConnectionsPerExecutor  = targetSettings.connections,
        remoteConnectionsPerExecutor = targetSettings.connections,
        queryRetryCount              = -1
      )
    )

  def getCassandraSSLConfFromSparkConf(conf: SparkConf): CassandraSSLConf = {
    val sslEnabled = conf.getBoolean(SSLEnabledParam.name, SSLEnabledParam.default)
    val sslTrustStorePath =
      conf.getOption(SSLTrustStorePathParam.name).orElse(SSLTrustStorePathParam.default)
    val sslTrustStorePassword =
      conf.getOption(SSLTrustStorePasswordParam.name).orElse(SSLTrustStorePasswordParam.default)
    val sslTrustStoreType = conf.get(SSLTrustStoreTypeParam.name, SSLTrustStoreTypeParam.default)
    val sslProtocol = conf.get(SSLProtocolParam.name, SSLProtocolParam.default)
    val sslEnabledAlgorithms = conf
      .getOption(SSLEnabledAlgorithmsParam.name)
      .map(_.split(",").map(_.trim).toSet)
      .getOrElse(SSLEnabledAlgorithmsParam.default)
    val sslClientAuthEnabled =
      conf.getBoolean(SSLClientAuthEnabledParam.name, SSLClientAuthEnabledParam.default)
    val sslKeyStorePath =
      conf.getOption(SSLKeyStorePathParam.name).orElse(SSLKeyStorePathParam.default)
    val sslKeyStorePassword =
      conf.getOption(SSLKeyStorePasswordParam.name).orElse(SSLKeyStorePasswordParam.default)
    val sslKeyStoreType = conf.get(SSLKeyStoreTypeParam.name, SSLKeyStoreTypeParam.default)

    CassandraSSLConf(
      enabled            = sslEnabled,
      trustStorePath     = sslTrustStorePath,
      trustStorePassword = sslTrustStorePassword,
      trustStoreType     = sslTrustStoreType,
      clientAuthEnabled  = sslClientAuthEnabled,
      keyStorePath       = sslKeyStorePath,
      keyStorePassword   = sslKeyStorePassword,
      keyStoreType       = sslKeyStoreType,
      protocol           = sslProtocol,
      enabledAlgorithms  = sslEnabledAlgorithms
    )
  }
}
