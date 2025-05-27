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

task_queue = queue.Queue()
python_path = "/usr/bin/python3"
script_path = ""


class My_Socket_Client:
    def __init__(self):
        self.client: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.rank = -1
        self.size = -1
        self.setup_info = []
        self.taskfilename = ""
        self.datafilename = ""

    def connect_server(self, IP_ADDR, IP_PORT):
        try:
            self.client.connect((IP_ADDR, IP_PORT))
            senddata = {"data_type": "client", "payload": "i am client"}
            self.send_data(senddata)
        except Exception as e:
            print(f"error code:{e}")

    def server_handle(self):
        """
        每个client处理来自control节点的消息
        """
        while True:
            data = self.client_recv_data()  # 接收来自control的消息
            if data:
                data_type = data.get("data_type")
                if data_type == "data":
                    if data.get("payload") == "close":
                        task_queue.put("close")
                        self.client.shutdown(socket.SHUT_RDWR)
                        self.client.close()
                        break
                    elif data.get("payload") == "GOON":
                        task_queue.put(self.taskfilename)
                elif data_type == "task_file":
                    self.save_task_file(data)
                elif data_type == "data_file":
                    self.save_data_file(data)
                elif data_type == "setup_file":
                    self.do_setup(data)
            else:
                break

    def client_recv_data(self) -> dict:
        """
        每个client节点接收来自control的消息
        """
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
        self.setup_info: list = json.loads(payload)
        self.size = len(self.setup_info)
        for config in self.setup_info:
            if config.get("ip") == self.client.getsockname()[0]:
                self.rank = config.get("rank")
                break

    def execute_task(self):
        while True:
            param = task_queue.get()
            if param == "close":
                break
            script_path = os.path.join(os.getcwd(), param)
            result = subprocess.run(
                [
                    python_path,
                    script_path,
                    str(self.rank),
                    str(self.size),
                    str(self.datafilename),
                    str(self.setup_info[0].get("ip")),
                    str(self.setup_info[0].get("port")),
                ],
                capture_output=True,
                text=True,
            )
            print(f"stderr:{result.stderr}")
            print(f"strout:{result.stdout}")
            if self.rank == 0:
                self.send_data(json.loads(result.stdout))

    def send_data(self, data):
        socket_data = pickle.dumps(data)
        data_len = struct.pack("!I", len(socket_data))
        self.client.sendall(data_len + socket_data)


if __name__ == "__main__":
    my_socket = My_Socket_Client()
    my_socket.connect_server("192.168.57.1", 54321)
    threading.Thread(target=my_socket.server_handle).start()
    threading.Thread(target=my_socket.execute_task).start()
