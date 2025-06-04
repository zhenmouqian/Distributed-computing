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
        self.no_rank0_client = None
        self.rank0_server = None
        self.client_list_of_rank0_server = []
        self.stage = 1
        self.rank = -1
        self.size = -1
        self.task = ""
        self.taskfilename = ""
        self.datafilename = ""
        self.rank0ip = ""
        self.rank0port = 0
        self.res = []
        self.cond = threading.Condition()
        self.rank0_server_started = False
        self.target_ips = []  # 当前任务目标客户端

    def connect_server(self, IP_ADDR, IP_PORT):
        try:
            self.client.connect((IP_ADDR, IP_PORT))
            senddata = {"data_type": "client", "payload": "i am client"}
            self.send_data(self.client, senddata)
        except Exception as e:
            print(f"error code:{e}")

    def start_accept_no_rank0_client(self):
        while True:
            conn, address = self.rank0_server.accept()
            threading.Thread(
                target=self.message_handle_for_rank0, args=(conn, address), daemon=True
            ).start()

    def message_handle_for_rank0(self, conn: socket.socket, address: tuple):
        """
        rank0节点处理规约数据
        """
        self.client_list_of_rank0_server.append(conn)
        while True:
            data = self.recv_data(conn)  # 接收需要进行规约的数据
            if data:
                if isinstance(data, dict):
                    if data.get("data_type") == "res":
                        self.res.append(data["payload"])
                        with self.cond:
                            self.cond.notify_all()
                else:
                    print("something wrong" + data)
            else:
                break

    def message_handle_for_no_rank0(self):
        """
        每个no_rank0节点处理来自rank0节点的消息
        for task2
        """
        while True:
            data = self.recv_data(self.no_rank0_client)
            if data and isinstance(data, dict):
                data_type = data.get("data_type")
                if "res" in data_type:
                    task_queue.put(
                        {
                            "file": self.taskfilename,
                            "stage": 2,
                            "param": data.get("payload"),
                        }
                    )
            else:
                break

    def recv_data(self, conn: socket.socket) -> dict:
        raw_len = conn.recv(4)
        if raw_len == b"":
            if conn in self.client_list_of_rank0_server:
                self.client_list_of_rank0_server.remove(conn)
            conn.close()
            return None
        data_length = struct.unpack("!I", raw_len)[0]
        recv_data = b""
        while len(recv_data) < data_length:
            remaining = data_length - len(recv_data)
            pack = conn.recv(min(4096, remaining))
            if not pack:
                break
            recv_data += pack
        data = pickle.loads(recv_data)
        return data

    def server_handle(self):
        """
        每个client处理来自control节点的消息
        """
        while True:
            data = self.recv_data(self.client)
            if data:
                data_type = data.get("data_type")
                if data_type == "data":
                    if data.get("payload").get("action") == "GOON":
                        self.target_ips = data.get("payload").get("target_ips")
                        my_ip = self.client.getsockname()[0]
                        print(self.target_ips, my_ip)
                        if my_ip in self.target_ips:
                            task_queue.put(
                                {"file": self.taskfilename, "stage": 1, "param": 0}
                            )
                        else:
                            print("本客户端未被选中参与本轮任务")
                    elif data.get("payload").get("action") == "close":
                        task_queue.put({"file": "", "stage": 0, "param": "close"})
                        self.client.shutdown(socket.SHUT_RDWR)
                        self.client.close()
                elif data_type == "task_file":
                    self.save_task_file(data)
                elif data_type == "data_file":
                    self.save_data_file(data)
                elif data_type == "setup_file":
                    self.do_setup(data)
            else:
                break

    def save_task_file(self, data: dict):
        filename = data.get("file_name_copy")
        payload = data.get("payload")
        with open(filename, "wb") as f:
            f.write(payload)
            self.taskfilename = filename
        print(f"任务文件{filename}接收完成.")
        self.task = filename[:5]

    def save_data_file(self, data: dict):
        filename = data.get("file_name_copy")
        payload = data.get("payload")
        with open(filename, "wb") as f:
            f.write(payload)
            self.datafilename = filename
        print(f"数据文件{filename}接收完成.")

    def do_setup(self, data: dict):
        filename: str = data.get("file_name_copy")
        setup_payload: bytes = data.get("payload")
        with open(filename, "wb") as f:
            f.write(setup_payload)
        print(f"配置文件{filename}接收完成.")

        payload_decode: list = json.loads(setup_payload)
        self.rank0ip = payload_decode[0].get("ip")
        self.rank0port = payload_decode[0].get("port")
        self.size = len(payload_decode)
        for config in payload_decode:
            if config.get("ip") == self.client.getsockname()[0]:
                self.rank = config.get("rank")
        if self.rank == 0:
            if not self.rank0_server_started:
                self.rank0_server: socket.socket = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM
                )
                self.rank0_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.rank0_server.bind((self.rank0ip, self.rank0port))
                self.rank0_server.listen(5)
                threading.Thread(target=self.start_accept_no_rank0_client).start()
                threading.Thread(target=self.get_res).start()
                self.rank0_server_started = True
        else:
            if self.no_rank0_client is None:
                sock: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.connect((self.rank0ip, self.rank0port))
                self.no_rank0_client: socket.socket = sock
                assert isinstance(self.no_rank0_client, socket.socket)
                threading.Thread(
                    target=self.message_handle_for_no_rank0, daemon=True
                ).start()

    def execute_task(self):
        while True:
            try:
                if self.rank != -1:
                    obj: dict = task_queue.get()
                    task_filename = obj.get("file")
                    param = obj.get("param")
                    self.stage = obj.get("stage")
                    if param == "close":
                        break
                    script_path = os.path.join(os.getcwd(), task_filename)
                    self.size = len(self.target_ips)
                    my_ip = self.client.getsockname()[0]
                    if my_ip in self.target_ips:
                        self.rank = self.target_ips.index(my_ip)
                    else:
                        print("[跳过] 当前客户端不在本轮目标中")
                        continue
                    print(f"stage = {str(self.stage)}")
                    result = subprocess.run(
                        [
                            python_path,
                            script_path,
                            str(self.rank),
                            str(self.size),
                            str(self.stage),
                            str(self.datafilename),
                            str(param),
                        ],
                        capture_output=True,
                        text=True,
                        cwd=os.path.dirname(script_path),
                    )
                    print(f"stderr:{result.stderr}")
                    print(f"strout:{result.stdout}")
                    if self.rank == 0:
                        self.res.append(result.stdout)
                        with self.cond:
                            self.cond.notify_all()
                    else:
                        if self.rank != -1:
                            senddata = {"data_type": "res", "payload": result.stdout}
                            self.send_data(self.no_rank0_client, senddata)
            except Exception as e:
                print(f"in execute_task exception {e}")

    def get_res(self):
        while True:
            with self.cond:
                if not self.target_ips:
                    continue
                while len(self.res) != len(self.target_ips):
                    self.cond.wait()
                if self.task == "task1":
                    task1_res = max(self.res)
                    self.send_data(
                        self.client, {"data_type": "task1_res", "payload": task1_res}
                    )
                else:
                    if self.stage == 1:
                        res = max(self.res)
                        for con in self.client_list_of_rank0_server:
                            try:
                                peer_ip = con.getpeername()[0]
                                if peer_ip in self.target_ips:
                                    self.send_data(
                                        con,
                                        {
                                            "data_type": self.task + "res1",
                                            "payload": res,
                                        },
                                    )
                            except Exception as e:
                                print(f"[广播失败] 跳过无效连接: {e}")
                        task_queue.put(
                            {"file": self.taskfilename, "stage": 2, "param": res}
                        )
                    elif self.stage == 2:
                        self.send_data(
                            self.client,
                            {"data_type": self.task + "_res", "payload": self.res},
                        )
                self.res = []

    def send_data(self, conn: socket.socket, data):
        socket_data = pickle.dumps(data)
        data_len = struct.pack("!I", len(socket_data))
        conn.sendall(data_len + socket_data)


if __name__ == "__main__":
    my_socket = My_Socket_Client()
    my_socket.connect_server("192.168.57.1", 54321)
    print(f"客户端1本地IP和端口{my_socket.client.getsockname()}")
    threading.Thread(target=my_socket.server_handle).start()
    threading.Thread(target=my_socket.execute_task).start()
