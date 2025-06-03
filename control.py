import socket
import json
import struct
import pickle
import asyncio
import threading
import datetime

Server_IP = "192.168.57.1"


class My_Socket_Server:
    def __init__(self, IP_ADDR: str, IP_PORT: int):
        self.clientlist = []
        self.clientnum = 0
        self.allclientOnline = False
        self.setup_info = None
        self.get_setupinfo()
        self.server: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((IP_ADDR, IP_PORT))
        self.server.listen(5)

    def get_setupinfo(self):
        with open("setup1.txt", "rb") as f:
            self.setup_info: list = json.load(f)

    def start(self):
        while True:
            conn, address = self.server.accept()
            threading.Thread(target=self.client_handle, args=(conn, address)).start()

    def client_handle(self, conn: socket.socket, address: tuple):
        while True:
            data = self.recv_data(conn, address)
            if data:
                if isinstance(data, dict):
                    data_type = data.get("data_type")
                    if data_type == "client":
                        self.newClientInit(conn, address)
                    elif data_type == "res":
                        self.endtime = datetime.datetime.now()
                        print(f"time:{self.endtime-self.starttime}")
                        print(f"Get final result : {data}")
                    # time:0:00:00.043251   1.1
                    # time:0:00:00.052753   1.2

                    # time:0:00:00.056767   2.1
                    # time:0:00:00.054463   2.2
            else:
                break

    def newClientInit(self, conn: socket.socket, address: tuple):
        print(f"new client online {address}")
        self.clientnum += 1
        if self.clientnum == len(self.setup_info):
            print("All client online,input GOON to start task")
            self.allclientOnline = True
        self.clientlist.append(conn)

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
            senddata = {"data_type": data_type, "payload": kw.get("payload")}
            socket_data = pickle.dumps(senddata)
            data_len = struct.pack("!I", len(socket_data))
            conn.sendall(data_len + socket_data)
        elif "file" in data_type:
            file_name: str = kw.get("file_name")
            with open(file_name, "rb") as file:
                senddata = {
                    "data_type": data_type,
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
        for con in self.clientlist:
            self.send_data(con, data_type="data", payload="GOON")

    async def GOON(self):
        loop = asyncio.get_event_loop()
        while True:
            user_input = await loop.run_in_executor(None, input)
            if user_input == "GOON":
                self.starttime = datetime.datetime.now()
                self.broadcast_goon()
            if user_input == "task1" and self.allclientOnline:
                for conn in self.clientlist:
                    self.send_data(conn, file_name="task1.py", data_type="task_file")
                    self.send_data(conn, file_name="setup1.txt", data_type="setup_file")
                    self.send_data(
                        conn, file_name="test_num.txt", data_type="data_file"
                    )
            if user_input == "task2" and self.allclientOnline:
                for conn in self.clientlist:
                    self.send_data(conn, file_name="task2.py", data_type="task_file")
                    self.send_data(conn, file_name="setup1.txt", data_type="setup_file")
                    self.send_data(
                        conn, file_name="test_num.txt", data_type="data_file"
                    )
            if user_input == "close":
                for conn in self.clientlist:
                    self.send_data(conn, data_type="data", payload="close")
                    self.clientnum = 0
                    self.clientlist = []
                    self.allclientOnline = False


if __name__ == "__main__":
    my_socket = My_Socket_Server(Server_IP, 54321)
    threading.Thread(target=my_socket.start).start()
    asyncio.run(my_socket.GOON())
