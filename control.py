import socket
import os
import struct
import pickle
import asyncio
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

    def start(self):
        while True:
            conn, address = self.server.accept()
            threading.Thread(
                target=self.client_handle, args=(conn, address), daemon=True
            ).start()

    def client_handle(self, conn: socket.socket, address: tuple):
        while True:
            data = self.recv_data(conn, address)
            if data:
                if isinstance(data, dict):
                    data_type = data.get("type")
                    if data_type == "client":
                        self.newClientInit(conn, address)
                    elif data_type == "res":
                        self.endtime = datetime.datetime.now()
                        print(f"time:{self.endtime-self.starttime}")
                        print(data["payload"])
                else:
                    self.endtime = datetime.datetime.now()
                    print(f"time:{self.endtime-self.starttime}")
                    # time:0:00:00.043207   2
                    # time:0:00:00.034833   1
                    print(data)
            else:
                break

    def newClientInit(self, conn: socket.socket, address: tuple):
        print(f"new client online {address}")
        print(f"send file task1.py")
        self.send_data(conn, file_name="task1.py", data_type="task_file")
        self.clientlist.append(conn)
        print(f"send file setup1.txt")
        self.send_data(conn, file_name="setup1.txt", data_type="setup_file")
        print(f"send file test_num.txt")
        self.send_data(conn, file_name="test_num.txt", data_type="data_file")

        self.clientnum += 1

    def recv_data(self, conn: socket.socket, address: tuple):
        raw_len = conn.recv(4)
        if raw_len == b"":
            print(f"client address:{address} closed")
            conn.close()
            return None
        data_len = struct.unpack("!I", raw_len)[0]
        recv_data = b""
        while len(recv_data) < data_len:
            pack = conn.recv(data_len)
            if not pack:
                break
            recv_data += pack
        data = pickle.loads(recv_data)
        return data

    def send_data(self, conn: socket.socket, **kw):
        data_type: str = kw.get("data_type")
        if data_type == "data":
            senddata = kw.get("data")
            socket_data = pickle.dumps(senddata)
            data_len = struct.pack("!I", len(socket_data))
            conn.sendall(data_len + socket_data)
        elif (
            data_type == "task_file"
            or data_type == "data_file"
            or data_type == "setup_file"
        ):
            file_name: str = kw.get("file_name")
            print(f"sending {file_name}")
            with open(file_name, "rb") as file:
                senddata = {
                    "type": data_type,
                    "file_name_copy": file_name[: file_name.index(".")]
                    + "_"
                    + str(self.clientnum)
                    + file_name[file_name.index(".") :],
                    "payload": file.read(),
                }
                socket_data = pickle.dumps(senddata)
                data_len = struct.pack("!I", len(socket_data))
                conn.sendall(data_len + socket_data)

    def broadcast_goon(self):
        senddata = {"type": "GOON", "payload": "GOON"}
        for con in self.clientlist:
            self.send_data(con, data=senddata, data_type="data")

    async def GOON(self):
        loop = asyncio.get_event_loop()
        user_input = await loop.run_in_executor(None, input)
        if user_input == "GOON":
            print("send goon")
            self.starttime = datetime.datetime.now()
            self.broadcast_goon()


if __name__ == "__main__":
    my_socket = My_Socket_Server("192.168.57.1", 54321)
    threading.Thread(target=my_socket.start).start()
    asyncio.run(my_socket.GOON())


# 0:00:00.063419
