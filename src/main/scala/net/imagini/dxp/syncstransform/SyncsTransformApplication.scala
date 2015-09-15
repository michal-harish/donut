package net.imagini.dxp.syncstransform

import net.imagini.dxp.common.DXPConfig
import org.apache.donut.DonutApp

/**
 * Created by mharis on 15/09/15.
 */
class SyncsTransformApplication extends DonutApp[SyncsTransformProcessUnit](512, false, "datasync") {
  val config = new DXPConfig {
    set("kafka.group.id", "SyncsToGraphTransformer")
    setBoolean("donut.bootstrap", false)
    set("yarn.name", "SyncsToGraphTransformer")
    set("yarn.queue", "developers")
    setInt("yarn.master.priority", 0)
    setLong("yarn.master.timeout.s", 3600L)
  }

  def runOnYarn: Unit = runOnYarn(config)

  def runLocally: Unit = runLocally(config)
}
