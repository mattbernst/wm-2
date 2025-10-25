package wiki.service

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*
import scala.sys.process.*
import scala.util.Try
import scala.util.matching.Regex

object JdwpDetector {

  def isDebugEnabled: Boolean = {
    val inputArguments = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala

    inputArguments.exists { arg =>
      arg.startsWith("-agentlib:jdwp") || arg.startsWith("-Xrunjdwp")
    }
  }

  def getDebugPort: Option[Int] = {
    // First try to get the actual runtime port by checking listening ports
    getRuntimeDebugPort.orElse(getConfiguredDebugPort)
  }

  private def getRuntimeDebugPort: Option[Int] = {
    if (!isDebugEnabled) return None

    Try {
      val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
      val os  = System.getProperty("os.name").toLowerCase

      val ports = if (os.contains("mac") || os.contains("darwin")) {
        // macOS
        val output = s"lsof -Pan -p $pid -iTCP -sTCP:LISTEN".!!
        parseLsofOutput(output)
      } else if (os.contains("linux")) {
        // Linux
        val output = s"lsof -Pan -p $pid -iTCP -sTCP:LISTEN".!!
        parseLsofOutput(output)
      } else if (os.contains("win")) {
        // Windows
        val output = s"netstat -ano".!!
        parseNetstatOutput(output, pid)
      } else {
        Set.empty[Int]
      }

      // Return the first port we find (there should only be one JDWP port)
      ports.headOption
    }.toOption.flatten
  }

  private def parseLsofOutput(output: String): Set[Int] = {
    // Example line: java    39782 user   14u  IPv6 0x1234  0t0  TCP *:57744 (LISTEN)
    val portPattern = """.*[:\.](\d+)\s+\(LISTEN\)""".r

    output.linesIterator.flatMap { line =>
      portPattern.findFirstMatchIn(line).map(_.group(1).toInt)
    }.toSet
  }

  private def parseNetstatOutput(output: String, pid: String): Set[Int] = {
    // Example line: TCP    0.0.0.0:57744    0.0.0.0:0    LISTENING    39782
    val linePattern = s"""TCP\\s+[^:]+:(\\d+)\\s+[^\\s]+\\s+LISTENING\\s+$pid""".r

    output.linesIterator.flatMap { line =>
      linePattern.findFirstMatchIn(line).map(_.group(1).toInt)
    }.toSet
  }

  private def getConfiguredDebugPort: Option[Int] = {
    val inputArguments        = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
    val addressPattern: Regex = """address=(\*:)?(\d+)""".r

    inputArguments.collectFirst {
      case arg if arg.startsWith("-agentlib:jdwp") || arg.startsWith("-Xrunjdwp") =>
        addressPattern.findFirstMatchIn(arg).map(m => m.group(2).toInt)
    }.flatten.filterNot(_ == 0) // Filter out port 0
  }
}
