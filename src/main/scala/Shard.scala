import java.security.cert.X509Certificate
import javax.net.ssl.{HostnameVerifier, SSLSession}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.HttpClient
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.ssl.{SSLContextBuilder, TrustStrategy}

class Shard {
  val options = new scala.collection.mutable.HashMap[String, String]

  def config(key: String, value: String): Shard = synchronized {
    options += key -> value
    this
  }

  def domain(value: String) = config("domain", value)
  def username(value: String) = config("username", value)
  def password(value: String) = config("password", value)
  def token(value: String) = username("token").password(value)

  def connect: ShardSession = {
    val client = Shard.client(options("username"), options("password"))
    ShardSession(client)
  }
}

object Shard {
  def apply: Shard = new Shard()

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
