/**
 * Copyright (C) 2015 Michal Harish
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
