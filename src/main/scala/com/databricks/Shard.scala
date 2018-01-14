package com.databricks

import java.security.cert.X509Certificate
import javax.net.ssl.{HostnameVerifier, SSLSession}

import com.typesafe.config.Config
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.HttpClient
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.ssl.{SSLContextBuilder, TrustStrategy}

class Shard extends Endpoint {
  val options = new scala.collection.mutable.HashMap[String, String]

  def config(key: String, value: String): Shard = synchronized {
    options += key -> value
    this
  }

  override def url: String = options("shard")

  def shard(value: String) = config("shard", value)
  def username(value: String) = config("username", value)
  def password(value: String) = config("password", value)
  def token(value: String) = username("token").password(value)

  def connect: ShardClient = {
    val client = Shard.client(options("username"), options("password"))
    ShardClient(client, options("shard"))
  }
}

object Shard {
  def apply(name: String): Shard = new Shard().shard(name)

  def apply(config: Config): Shard = {
    new Shard()
      .shard(config.getString("shard.url"))
      .username(config.getString("credentials.username"))
      .password(config.getString("credentials.password"))
  }

  def client(username: String, password: String): HttpClient = {
    val sslContext = new SSLContextBuilder().loadTrustMaterial(new TrustStrategy() {
      def isTrusted(arg0: Array[X509Certificate], arg1: String): Boolean = true
    }).build()

    val hostnameVerifier = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }

    val sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier)
    val socketFactoryRegistry = RegistryBuilder.create[ConnectionSocketFactory]()
      .register("https", sslSocketFactory)
      .register("http", PlainConnectionSocketFactory.getSocketFactory)
      .build()
    val connMgr = new PoolingHttpClientConnectionManager(socketFactoryRegistry)

    val provider = new BasicCredentialsProvider
    val credentials = new UsernamePasswordCredentials(username, password)
    provider.setCredentials(AuthScope.ANY, credentials)

    HttpClients.custom()
      .setConnectionManager(connMgr)
      .setDefaultCredentialsProvider(provider)
      .build()
  }
}
