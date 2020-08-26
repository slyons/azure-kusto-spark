package com.microsoft.kusto.spark.utils

import java.util.concurrent.ConcurrentHashMap
import java.util.function

import com.microsoft.azure.kusto.data.ConnectionStringBuilder
import com.microsoft.kusto.spark.authentication.{AadApplicationAuthentication, KeyVaultAuthentication, KustoAccessTokenAuthentication, KustoAuthentication, KustoLSRAuthentication}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils, KustoConstants => KCONST}

object KustoClientCache {
  var clientCache = new ConcurrentHashMap[AliasAndAuth, KustoClient]

  def getClient(clusterAlias: String, clusterUrl: String, authentication: KustoAuthentication): KustoClient = {
    val clusterAndAuth = AliasAndAuth(clusterAlias, clusterUrl, authentication)
    clientCache.computeIfAbsent(clusterAndAuth, adderSupplier)
  }

  val adderSupplier: function.Function[AliasAndAuth, KustoClient] = new java.util.function.Function[AliasAndAuth, KustoClient]() {
    override def apply(aa: AliasAndAuth): KustoClient = createClient(aa)
  }

  private def createClient(aliasAndAuth: AliasAndAuth): KustoClient = {
    val resolvedAuthentication = aliasAndAuth.authentication match {
      case null => throw new MatchError("Can't create ConnectionStringBuilder with null authentication params")
      case keyVaultParams: KustoLSRAuthentication =>
        KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultParams)
      case keyVaultParams: KeyVaultAuthentication =>
        KeyVaultUtils.getAadAppParametersFromKeyVault(keyVaultParams)
      case _ => aliasAndAuth.authentication
    }

    KustoDataSourceUtils.logError("createClient", s"resolvedAuthentication $resolvedAuthentication")

    val (engineKcsb, ingestKcsb) = resolvedAuthentication match {
      case null => throw new MatchError("Can't create ConnectionStringBuilder with null authentication params")
      case app: AadApplicationAuthentication => (
        ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.engineUri, app.ID, app.password, app.authority),
        ConnectionStringBuilder.createWithAadApplicationCredentials(aliasAndAuth.ingestUri, app.ID, app.password, app.authority)
      )
      case userToken: KustoAccessTokenAuthentication => 
        KustoDataSourceUtils.logError("createClient with token", s"link:${aliasAndAuth.engineUri}, token: $userToken")
        (
          ConnectionStringBuilder.createWithAadAccessTokenAuthentication(aliasAndAuth.engineUri, userToken.token),
          ConnectionStringBuilder.createWithAadAccessTokenAuthentication(aliasAndAuth.ingestUri, userToken.token)
        )
    }

    engineKcsb.setClientVersionForTracing(KCONST.clientName)
    ingestKcsb.setClientVersionForTracing(KCONST.clientName)

    new KustoClient(aliasAndAuth.clusterAlias, engineKcsb, ingestKcsb)
  }

  private[kusto] case class AliasAndAuth(clusterAlias: String, engineUrl: String, authentication: KustoAuthentication) {
    val engineUri: String = engineUrl
    val ingestUri: String = engineUrl.replace("https://", KustoDataSourceUtils.ingestPrefix)

    override def equals(that: Any): Boolean = that match {
      case aa: AliasAndAuth => clusterAlias == aa.clusterAlias && authentication == aa.authentication
      case _ => false
    }

    override def hashCode(): Int = clusterAlias.hashCode + authentication.hashCode
  }
}


