import socket
import time
import argparse
from contextlib import ExitStack

parser = argparse.ArgumentParser()

parser.add_argument('-p','--port', help='the port where to serve')
parser.add_argument('-f','--files', nargs='+', help='the file(s) to serve')

args = parser.parse_args()

serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = int(args.port)
serv.bind(('0.0.0.0', port))
serv.listen(5)

stream_files = args.files

print('streaming files', *stream_files, 'on port', port)
print('waiting for client...')
try:
    conn, addr = serv.accept()
    print('client', addr, 'connected')

    with ExitStack() as stack:
        files = [stack.enter_context(open(fname)) for fname in stream_files]

        for line in files[0]:
            lines = [line]
            for f in files[1:]:
                lines.append(next(f))

            time.sleep(0.001 / len(files))

            [conn.send(line.encode('UTF-8')) for line in lines]

finally:
    conn.close()
    serv.close()
    print('client disconnected')
