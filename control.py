import socket
import os
import struct
import pickle
import subprocess
import threading
import datetime
from time import sleep


class My_Socket_Server:
    def __init__(self, IP_ADDR: str, IP_PORT: int):
        self.server: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((IP_ADDR, IP_PORT))
        self.server.listen(5)
        self.clientlist = []
        self.clientnum = 0

    def client_handle(self, conn: socket.socket, address: tuple):
        while True:
            raw_len = conn.recv(4)
            if raw_len == b"":
                print(f"client address:{address} closed")
                conn.close()
                break
            data_len = struct.unpack("!I", raw_len)[0]
            recv_data = b""
            while len(recv_data) < data_len:
                pack = conn.recv(data_len)
                if not pack:
                    break
                recv_data += pack
            data = pickle.loads(recv_data)
            if isinstance(data, dict):
                if data["type"] == "client":
                    print(f"new client online {address}")
                    self.clientlist.append(conn)

                    self.send_data(
                        conn, {"type": "client_rank", "payload": self.clientnum}
                    )
                    self.send_file(conn, "task1.py", "execute_file")
                    self.send_file(conn, "test_num.txt", "data_file")

                    self.clientnum += 1

                elif data["type"] == "res":
                    self.endtime = datetime.datetime.now()
                    print(f"time:{self.endtime-self.starttime}")
                    print(data["payload"])
            else:
                print(data)

    def GOON(self):
        while True:
            raw = str(input())
            if raw == "GOON":
                senddata = {"type": "client_size", "payload": self.clientnum}
                for con in self.clientlist:
                    self.send_data(con, senddata)
                self.starttime = datetime.datetime.now()
                senddata = {"type": "GOON", "payload": "GOON"}
                for con in self.clientlist:
                    self.send_data(con, senddata)
                break

    def start(self):
        while True:
            conn, address = self.server.accept()
            threading.Thread(
                target=self.client_handle, args=(conn, address), daemon=True
            ).start()

    def send_file(self, conn: socket.socket, file_name: str, filetype: str):
        with open(file_name, "rb") as file:
            senddata = {
                "type": filetype,
                "file_name_copy": file_name[: file_name.index(".")]
                + "_"
                + str(self.clientnum)
                + file_name[file_name.index(".") :],
                "payload": file.read(),
            }
            socket_data = pickle.dumps(senddata)
            data_len = struct.pack("!I", len(socket_data))
            conn.sendall(data_len)
            conn.sendall(socket_data)

    def send_data(self, conn: socket.socket, senddata):
        socket_data = pickle.dumps(senddata)
        data_len = struct.pack("!I", len(socket_data))
        conn.sendall(data_len)
        conn.sendall(socket_data)


if __name__ == "__main__":
    my_socket = My_Socket_Server("192.168.57.1", 54321)
    threading.Thread(target=my_socket.GOON).start()
    my_socket.start()


# 0:00:00.063419
