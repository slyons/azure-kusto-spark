package com.microsoft.kusto.spark.utils

import java.io.IOException

import com.azure.security.keyvault.secrets.SecretClient
import com.microsoft.kusto.spark.authentication._
import com.microsoft.kusto.spark.datasource._

import scala.util.{Try}

object KeyVaultUtils {
  val AppId = "kustoAppId"
  val AppKey = "kustoAppKey"
  val AppAuthority = "kustoAppAuthority"
  val SasUrl = "blobStorageSasUrl"
  val StorageAccountName = "blobStorageAccountName"
  val StorageAccountKey = "blobStorageAccountKey"
  val Container = "blobContainer"
  var cachedClient: SecretClient = _

  private def getClient(uri: String, clientID: String, clientPassword: String, authority: String): SecretClient ={
    if(cachedClient == null) {
      cachedClient = new KeyVaultADALAuthenticator(uri, clientID, clientPassword, authority).getAuthenticatedClient
    }
    cachedClient
  }

  @throws[IOException]
  def getStorageParamsFromKeyVault(keyVaultAuthentication: KeyVaultAuthentication): TransientStorageCredentials = {
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.uri, app.keyVaultAppID, app.keyVaultAppKey, app.authority)
        getStorageParamsFromKeyVaultImpl(client, app.uri)
      case certificate: KeyVaultCertificateAuthentication => throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  @throws[IOException]
  def getAadAppParametersFromKeyVault(keyVaultAuthentication: KeyVaultAuthentication): AadApplicationAuthentication={
    keyVaultAuthentication match {
      case app: KeyVaultAppAuthentication =>
        val client = getClient(app.uri, app.keyVaultAppID, app.keyVaultAppKey, app.authority)
        getAadAppParamsFromKeyVaultImpl(client, app.uri)
      case _: KeyVaultCertificateAuthentication => throw new UnsupportedOperationException("certificates are not yet supported")
    }
  }

  private def getAadAppParamsFromKeyVaultImpl(client: SecretClient, uri: String): AadApplicationAuthentication ={
    val id = client.getSecret(AppId)
    val key = client.getSecret(AppKey)

    var authority :Option[String] = None
    try{
      authority = Some(client.getSecret(AppAuthority).getValue)
    } catch {
      case e:Exception => {
        println(e)
      }
    }
    if(authority.isEmpty){
      authority = Some("microsoft.com")
    }

    AadApplicationAuthentication(
      ID = if (id == null) null else id.getValue,
      password = if (key == null) null else key.getValue,
      authority = authority.get)
  }

  private def getStorageParamsFromKeyVaultImpl(client: SecretClient, uri: String): TransientStorageCredentials = {
    val sasUrl = Try(client.getSecret(SasUrl))

    val accountName =  Try(client.getSecret(StorageAccountName))
    val accountKey = Try(client.getSecret(StorageAccountKey))
    val container = Try(client.getSecret(Container))

    if(sasUrl.isFailure) {
      new TransientStorageCredentials(
        if(accountName.isFailure) accountName.get.getValue else "",
        if (accountKey.isFailure) accountKey.get.getValue else "",
        if (container.isFailure) container.get.getValue else "")
    } else {
      new TransientStorageCredentials(sasUrl.get.getValue)
    }
  }
}
