import socket
import time
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-p","--port", help="the port where to serve")
parser.add_argument("-f","--file", help="the file to serve")

args = parser.parse_args()

serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = int(args.port)
serv.bind(('0.0.0.0', port))

#serv.bind(('0.0.0.0', 10003))
serv.listen(5)

stream_file = args.file
#stream_file = "acc00.csv"

with open(stream_file,'r') as f:
    print("streaming file",stream_file,"on port",port)
    try:
        conn, _ = serv.accept()
        for line in f:
            time.sleep(0.001)

            encoded = line.encode("UTF-8")
            conn.sendall(encoded)

    finally:
        conn.close()
        serv.close()
        print('client disconnected')
