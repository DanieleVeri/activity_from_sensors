import java.io.{BufferedReader, FileReader, PrintStream}
import java.net.ServerSocket

val filename1 = "/home/dan/activity_from_sensors/data/allenamento/xab2"
val filename2 = "/home/dan/activity_from_sensors/data/allenamento/xae"
val server = new ServerSocket(7777)

while (true) {
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream)

    val file1 = new BufferedReader(new FileReader(filename1))
    val file2 = new BufferedReader(new FileReader(filename2))

    var i = 0
    while(true) {
        var line: String = ""
        if (i%2 == 0) {
            Thread.sleep(1)
            line = file1.readLine()
        } else {
            line = file2.readLine()
        }
        if (line.split(',')(0) != "Index") {
            out.println(line)
            out.flush()
        }
        i += 1
    }

    file1.close()
    file2.close()
    s.close()
}
