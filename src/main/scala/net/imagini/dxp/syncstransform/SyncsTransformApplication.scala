package net.imagini.dxp.syncstransform

import net.imagini.dxp.common.DXPConfig
import org.apache.donut.DonutApp
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 15/09/15.
 */
class SyncsTransformApplication(config: Configuration) extends DonutApp[SyncsTransformProcessUnit](config) {
  def this() = this(new DXPConfig {
    set("kafka.group.id", "SyncsToGraphTransformer")
    set("kafka.topics", "datasync")
    setBoolean("kafka.cogroup", false)
    set("yarn.name", "SyncsToGraphTransformer")
    set("yarn.queue", "developers")
    setInt("yarn.master.priority", 0)
    setLong("yarn.master.timeout.s", 3600L)
  })

  //FIXME memory leak - even 2 GBs will kill container but this is a simple transformation 256Mb should be enough
  def runOnYarn: Unit = runOnYarn(taskMemoryMb = 4 * 1024 )

  def runLocally: Unit = runLocally(multiThreadMode = false)
}
