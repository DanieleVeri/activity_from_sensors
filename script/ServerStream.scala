import java.io.{BufferedReader, FileReader, PrintStream}
import java.net.ServerSocket

val filename1 = "/home/dan/activity_from_sensors/data/accgyr_stream.csv"
val server = new ServerSocket(7777)

while (true) {
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream)

    val file1 = new BufferedReader(new FileReader(filename1))

    while(true) {
        Thread.sleep(1)
        val line = file1.readLine()
        if (line.split(',')(0) != "Index") {
            out.println(line)
            out.flush()
        }
    }

    file1.close()
    s.close()
}
