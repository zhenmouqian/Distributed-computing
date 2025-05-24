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
        self.no_rank0_client = None
        self.rank0_server = None
        self.client_list_of_rank0_server = []
        self.rank = -1
        self.size = -1
        self.taskfilename = ""
        self.datafilename = ""
        self.sinkNodeIp = ""
        self.sinkNodeport = 0
        self.res = []
        self.cond = threading.Condition()

    def connect_server(self, IP_ADDR, IP_PORT):
        try:
            self.client.connect((IP_ADDR, IP_PORT))
            senddata = {"type": "client", "payload": "i am client"}
            self.send_data(data=senddata, data_type="client_online")
        except Exception as e:
            print(f"error code:{e}")

    def start_accept_no_rank0_client(self):
        while True:
            conn, address = self.rank0_server.accept()
            print(f"new client{conn}")
            threading.Thread(
                target=self.client_handle_for_rank0, args=(conn, address), daemon=True
            ).start()

    def client_handle_for_rank0(self, conn: socket.socket, address: tuple):
        """
        rank为0的节点处理规约数据
        """
        self.client_list_of_rank0_server.append(conn)
        while True:
            data = self.server_recv_data_for_rank0(conn)  # 接收需要进行规约的数据
            if data:
                if isinstance(data, dict):
                    if data.get("type") == "res":
                        self.res.append(int(data["payload"]))
                        print(data["payload"])
                        with self.cond:
                            self.cond.notify_all()
                else:
                    print("something wrong" + data)
            else:
                break

    def server_recv_data_for_rank0(self, conn) -> dict:
        """
        rank0节点接收需要规约的数据
        """
        raw_len = conn.recv(4)
        if raw_len == b"":
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

    def server_handle_for_no_rank0(self):
        """
        每个no_rank0节点处理来自rank0节点的消息
        for task2
        """
        while True:
            data = self.no_rank0_client_recv_data()
            print(data)

    def no_rank0_client_recv_data(self):
        """
        每个no_rank0节点接收来自rank0节点的消息
        """
        raw_length = self.no_rank0_client.recv(4)
        if raw_length == b"":
            self.client.close()
            return None
        data_length = struct.unpack("!I", raw_length)[0]
        recv_data = b""
        while len(recv_data) < data_length:
            pack = self.no_rank0_client.recv(data_length)
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
            data = self.client_recv_data()  # 接收来自control的消息
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
                    self.rank0_server: socket.socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.rank0_server.bind((self.sinkNodeIp, self.sinkNodeport))
                    self.rank0_server.listen(5)
                    threading.Thread(target=self.start_accept_no_rank0_client).start()
                    threading.Thread(target=self.send_final_data).start()
                else:
                    print(
                        "connect to sink node",
                        f"{self.sinkNodeIp},{self.sinkNodeport},{self.rank}",
                    )
                    self.no_rank0_client: socket.socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.no_rank0_client.connect((self.sinkNodeIp, self.sinkNodeport))
                    threading.Thread(target=self.server_handle_for_no_rank0).start()
                filepath_queue.put(self.taskfilename)

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

        payload_decode: list = json.loads(payload)
        self.task = payload_decode[0].get("task")
        self.sinkNodeIp = payload_decode[1].get("ip")
        self.sinkNodeport = payload_decode[1].get("port")
        self.size = len(payload_decode) - 1
        for config in payload_decode:
            if (
                "ip" in config.keys()
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
        if self.rank == 0:
            self.res.append(int(result.stdout))
            print(f"my result {int(result.stdout)}")
            with self.cond:
                self.cond.notify_all()
        else:
            senddata = {"type": "res", "payload": int(result.stdout)}
            self.send_data(data=senddata, data_type="reduce_data")

    def send_data(self, **kw):
        data_type: str = kw.get("data_type")
        senddata = kw.get("data")
        socket_data = pickle.dumps(senddata)
        data_len = struct.pack("!I", len(socket_data))
        if (
            data_type == "client_online" or data_type == "final_res"
        ):  # 向control节点发送上线信息或者最终结果
            self.client.sendall(data_len + socket_data)
        elif data_type == "reduce_data":
            # 向rank为0的节点发送自己节点计算结果
            self.no_rank0_client.sendall(data_len + socket_data)
        elif data_type == "task1res":
            # task2 向rank非0的节点发送最大值结果
            kw.get("conn").sendall(data_len + socket_data)

    def send_final_data(self):
        with self.cond:
            while len(self.res) != self.size:
                self.cond.wait()
            if self.task == "task1":
                res1 = max(self.res)
                self.send_data(data=res1, data_type="final_res")
                self.client.close()
            elif self.task == "task2":
                res1 = max(self.res)
                for con in self.client_list_of_rank0_server:
                    print("send res1")
                    print(con)
                    self.send_data(data=res1, data_type="task1res", conn=con)
                pass


if __name__ == "__main__":
    my_socket = My_Socket_Client()
    my_socket.connect_server("192.168.57.1", 54321)
    print(f"客户端1本地IP和端口{my_socket.client.getsockname()}")
    threading.Thread(target=my_socket.server_handle).start()
    threading.Thread(target=my_socket.execute_task).start()
