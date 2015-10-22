package io.amient.utils

import java.net.NetworkInterface
import scala.collection.JavaConverters._

/**
 * Created by mharis on 22/10/15.
 */
object AddressUtils {
  def getLANHost(prefHost: String = null) = {
    if (prefHost != null) prefHost else {
      val addresses = NetworkInterface.getNetworkInterfaces.asScala.flatMap(_.getInterfaceAddresses.asScala).map(_.getAddress)
      val availableAddresses = addresses.filter(_.isSiteLocalAddress).toList
      val namedHosts = availableAddresses.filter(a => a.getHostAddress != a.getCanonicalHostName)
      if (!namedHosts.isEmpty) {
        namedHosts.head.getCanonicalHostName
      } else if (!availableAddresses.isEmpty) {
        availableAddresses.head.getHostAddress
      } else {
        "localhost"
      }
    }
  }
}
