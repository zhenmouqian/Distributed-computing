import queue
import socket
import pickle
import struct
import threading
import subprocess
import os
from time import sleep
import asyncio

filepath_queue = queue.Queue()
python_path = "/usr/bin/python3"
script_path = ""


class My_Socket_Client:
    def __init__(self):
        self.client: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.rank = -1
        self.size = -1
        self.myfilename = ""
        self.getres = 0
        self.res = []

    def connect_server(self, IP_ADDR, IP_PORT):
        try:
            self.client.connect((IP_ADDR, IP_PORT))
        except socket.timeout:
            print("超时")
        except Exception as e:
            print(f"error code:{e}")
        finally:
            senddata = {"type": "client"}
            self.send_data(senddata)

    def recv_data(self):
        while True:
            raw_length = self.client.recv(4)
            if len(raw_length) < 4:
                raise RuntimeError("接收到的数据长度不足")
            data_length = struct.unpack("!I", raw_length)[0]
            recv_data = b""
            while len(recv_data) < data_length:
                pack = self.client.recv(data_length)
                if not pack:
                    break
                recv_data += pack
            data = pickle.loads(recv_data)
            if data["type"] == "execute_file":
                with open(data["file_name_copy"], "wb") as f:
                    f.write(data["payload"])
                    self.exefilename = data["file_name_copy"]
                print("执行文件接收完成，等待所有客户端上线后执行")

            elif data["type"] == "data_file":
                with open(data["file_name_copy"], "wb") as f:
                    f.write(data["payload"])
                    self.datafilename = data["file_name_copy"]
                    print(self.datafilename)
                print("数据文件接收完成，等待所有客户端上线后执行")

            elif data["type"] == "client_rank":
                print(f'my rank is {data["payload"]}')
                self.rank = data["payload"]
                print(self.client.getsockname()[0])
                if self.client.getsockname()[0] == "192.168.57.128":
                    self.server: socket.socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.server.bind((self.client.getsockname()[0], 65432))
                    self.server.listen(5)
                    threading.Thread(target=self.start).start()
                    threading.Thread(target=self.sendreducedata).start()
                else:
                    self.reduceclient: socket.socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    self.reduceclient.connect(("192.168.57.128", 65432))
            elif data["type"] == "client_size":
                print(f'total client num {data["payload"]}')
                self.size = int(data["payload"])
            elif data["type"] == "GOON":
                filepath_queue.put(self.exefilename)

    def execute_file(self):
        filename = filepath_queue.get()
        print(f"{filename} is going to be executed")
        script_path = os.path.join(os.getcwd(), filename)
        result = subprocess.run(
            [
                python_path,
                script_path,
                str(self.rank),
                str(self.size),
                str(self.exefilename),
                str(self.datafilename),
            ],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(script_path),
        )
        senddata = {"type": "res", "payload": int(result.stdout)}
        if self.client.getsockname()[0] == "192.168.57.128":
            print(result.stdout)
            self.res.append(int(result.stdout))
            self.getres += 1
        else:
            self.sendreduce_data(senddata)

    def send_data(self, res):
        send_data = pickle.dumps(res)
        send_data_len = struct.pack("!I", len(send_data))
        self.client.sendall(send_data_len)
        self.client.sendall(send_data)

    def sendreduce_data(self, res):
        send_data = pickle.dumps(res)
        send_data_len = struct.pack("!I", len(send_data))
        self.reduceclient.sendall(send_data_len)
        self.reduceclient.sendall(send_data)

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
                if data["type"] == "res":
                    self.getres += 1
                    self.res.append(int(data["payload"]))
                    print("get reduce data", data["payload"])
            else:
                print(data)

    def sendreducedata(self):
        # print(f"self.getres{self.getres},self.size{self.size}")
        while True:
            if self.getres == self.size:
                print("send reduce data", self.res)
                self.send_data(max(self.res))
                break

    def start(self):
        while True:
            conn, address = self.server.accept()
            threading.Thread(
                target=self.client_handle, args=(conn, address), daemon=True
            ).start()


if __name__ == "__main__":
    my_socket = My_Socket_Client()
    my_socket.connect_server("192.168.57.1", 54321)
    print(f"客户端1本地IP和端口{my_socket.client.getsockname()}")
    threading.Thread(target=my_socket.recv_data).start()
    threading.Thread(target=my_socket.execute_file).start()
