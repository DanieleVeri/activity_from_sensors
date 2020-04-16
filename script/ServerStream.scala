import java.net._
import java.io._
import scala.io.Source

// Simple server of filenames lines to localhost:port

val server = new ServerSocket(7777)
val filename = "/path/to/file.csv"

while (true) {
  val s = server.accept()

  val out = new PrintStream(s.getOutputStream)

  var last_time = 0L
  val file = Source.fromFile(filename)

  for (line <- file.getLines) {
    val this_time = line.split(',')(1)
    if (line.split(',')(0) != "Index") {
      if (last_time != 0L) Thread.sleep(this_time.toLong - last_time)

      println(line)
      out.println(line)
      out.flush()
      last_time = this_time.toLong
    }
  }
  file.close()
  s.close()
}
