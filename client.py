import queue
import socket
import pickle
import struct
import threading
import subprocess
import os
from time import sleep
import asyncio
import json

filepath_queue = queue.Queue()
python_path = "/usr/bin/python3"
script_path = ""


class My_Socket_Client:
    def __init__(self):
        self.client: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reduceclient = None
        self.rank = -1
        self.size = -1
        self.myfilename = ""
        self.taskfilename = ""
        self.datafilename = ""
        self.sinkNodeIp = ""
        self.sinkNodeport = 0
        self.getres = 0
        self.res = []
        self.client_list = []
        self.cond = threading.Condition()

    def connect_server(self, IP_ADDR, IP_PORT):
        try:
            self.client.connect((IP_ADDR, IP_PORT))
            senddata = {"type": "client", "payload": "i am client"}
            self.send_data(data=senddata, data_type="client_online")
        except Exception as e:
            print(f"error code:{e}")

    def client_recv_data(self) -> dict:
        raw_length = self.client.recv(4)
        if raw_length == b"":
            self.client.close()
            return None
        data_length = struct.unpack("!I", raw_length)[0]
        recv_data = b""
        while len(recv_data) < data_length:
            pack = self.client.recv(data_length)
            if not pack:
                break
            recv_data += pack
        data = pickle.loads(recv_data)
        return data

    def client_handle(self, conn: socket.socket, address: tuple):
        while True:
            self.client_list.append(conn)
            data = self.server_recv_data(conn, address)
            if isinstance(data, dict):
                if data.get("type") == "res":
                    self.getres += 1
                    self.res.append(int(data["payload"]))
                    with self.cond:
                        self.cond.notify_all()
            else:
                print(data)

    def server_recv_data(self, conn, address) -> dict:
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
        print(data)
        return data

    def server_handle(self):
        while True:
            data = self.client_recv_data()
            data_type = data.get("type")
            print(data_type)
            if data_type == "task_file":
                self.save_task_file(data)
            elif data_type == "data_file":
                self.save_data_file(data)
            elif data_type == "setup_file":
                self.do_setup(data)
            elif data_type == "GOON":
                if self.rank == 0:
                    self.server: socket.socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.server.bind((self.sinkNodeIp, self.sinkNodeport))
                    self.server.listen(5)
                    threading.Thread(target=self.start).start()
                    threading.Thread(target=self.send_final_data).start()
                else:
                    print(
                        "connect to sink node",
                        f"{self.sinkNodeIp},{self.sinkNodeport},{self.rank}",
                    )
                    self.reduceclient: socket.socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.reduceclient.connect((self.sinkNodeIp, self.sinkNodeport))

                filepath_queue.put(self.taskfilename)

    def save_task_file(self, data: dict):
        filename = data.get("file_name_copy")
        payload = data.get("payload")
        with open(filename, "wb") as f:
            f.write(payload)
            self.taskfilename = filename
        print(f"任务文件{filename}接收完成.")

    def save_data_file(self, data: dict):
        filename = data.get("file_name_copy")
        payload = data.get("payload")
        with open(filename, "wb") as f:
            f.write(payload)
            self.datafilename = filename
        print(f"数据文件{filename}接收完成.")

    def do_setup(self, data: dict):
        filename: str = data.get("file_name_copy")
        payload: bytes = data.get("payload")
        with open(filename, "wb") as f:
            f.write(payload)
        print(f"配置文件{filename}接收完成.")

        payload_decode: list = json.loads(payload)
        self.task = payload_decode[0].get("task")
        self.sinkNodeIp = payload_decode[1].get("ip")
        self.sinkNodeport = payload_decode[1].get("port")
        self.size = len(payload_decode)
        for config in payload_decode:
            if (
                "ip" in config.key()
                and config.get("ip") == self.client.getsockname()[0]
            ):
                self.rank = config.get("rank")
                break

    def execute_task(self):
        task_filename = filepath_queue.get()
        print(f"{task_filename} is going to be executed")
        script_path = os.path.join(os.getcwd(), task_filename)
        result = subprocess.run(
            [
                python_path,
                script_path,
                str(self.rank),
                str(self.size),
                str(self.taskfilename),
                str(self.datafilename),
            ],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(script_path),
        )
        senddata = {"type": "res", "payload": int(result.stdout)}
        if self.rank == 0:
            self.res.append(int(result.stdout))
            self.getres += 1
            with self.cond:
                self.cond.notify_all()
        else:
            self.send_data(data=senddata, data_type="reduce_data")

    def send_data(self, **kw):
        data_type: str = kw.get("data_type")
        senddata = kw.get("data")
        socket_data = pickle.dumps(senddata)
        data_len = struct.pack("!I", len(socket_data))
        if data_type == "client_online" or data_type == "server_res":
            self.client.sendall(data_len + socket_data)
        elif data_type == "reduce_data":
            self.reduceclient.sendall(data_len + socket_data)

    def send_final_data(self):
        with self.cond:
            while self.getres != self.size:
                self.cond.wait()

            if self.task == "task1":
                self.send_data(data=max(self.res), data_type="server_res")
            elif self.task == "task2":
                pass
            self.client.close()

    def start(self):
        while True:
            conn, address = self.server.accept()
            print("new client")
            threading.Thread(
                target=self.client_handle, args=(conn, address), daemon=True
            ).start()


if __name__ == "__main__":
    my_socket = My_Socket_Client()
    my_socket.connect_server("192.168.57.1", 54321)
    print(f"客户端1本地IP和端口{my_socket.client.getsockname()}")
    threading.Thread(target=my_socket.server_handle).start()
    threading.Thread(target=my_socket.execute_task).start()
