package wiki.service

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex

object JdwpDetector {

  def isDebugEnabled: Boolean = {
    val inputArguments = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala

    inputArguments.exists { arg =>
      arg.startsWith("-agentlib:jdwp") || arg.startsWith("-Xrunjdwp")
    }
  }

  def getDebugPort: Option[Int] = {
    val inputArguments = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala

    val addressPattern: Regex = """address=(\*:)?(\d+)""".r

    inputArguments.collectFirst {
      case arg if arg.startsWith("-agentlib:jdwp") || arg.startsWith("-Xrunjdwp") =>
        addressPattern.findFirstMatchIn(arg).map(m => m.group(2).toInt)
    }.flatten
  }
}
