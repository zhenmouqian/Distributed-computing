import os
import sys
import socket
import threading
import pickle
import struct
import json


class My_Cal_Node:
    def __init__(self):
        self.cond = threading.Condition()
        self.rank = int(sys.argv[1])
        self.size = int(sys.argv[2])
        self.datafilename = str(sys.argv[3])
        self.rank0ip = str(sys.argv[4])
        self.rank0port = int(sys.argv[5])
        if self.rank == 0:
            self.res = []
            self.client_list_of_rank0_server = []
            self.rank0_server: socket.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            )
            self.rank0_server.bind((self.rank0ip, self.rank0port))
            self.rank0_server.listen(5)
            threading.Thread(target=self.start_accept_no_rank0_client).start()
            threading.Thread(target=self.get_final_res).start()
        elif self.size != 1:
            self.no_rank0_client: socket.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            )
            self.no_rank0_client.connect((self.rank0ip, self.rank0port))
        self.do_task1()

    def start_accept_no_rank0_client(self):
        while True:
            conn, address = self.rank0_server.accept()
            threading.Thread(
                target=self.message_handle_for_rank0, args=(conn, address), daemon=True
            ).start()

    def message_handle_for_rank0(self, conn: socket.socket, address: tuple):
        """
        rank为0的节点处理规约数据
        """
        self.client_list_of_rank0_server.append(conn)
        while True:
            data = self.recv_data_for_rank0(conn)  # 接收需要进行规约的数据
            if data:
                if isinstance(data, dict):
                    if data.get("data_type") == "res":
                        self.res.append(int(data["payload"]))
                        with self.cond:
                            self.cond.notify_all()
                else:
                    print("something wrong" + data)
            else:
                break

    def recv_data_for_rank0(self, conn: socket.socket) -> dict:
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

    def get_final_res(self):
        with self.cond:
            while len(self.res) != self.size:
                self.cond.wait()
            res = {"data_type": "res", "task": "task1", "res1": (max(self.res))}
            print(json.dumps(res), flush=True, end="")
        os._exit(0)

    def send_data(self, con: socket.socket, data):
        socket_data = pickle.dumps(data)
        data_len = struct.pack("!I", len(socket_data))
        con.sendall(data_len + socket_data)

    def do_task1(self):
        maxnum = -1
        all_nums = []
        cnt = 0
        with open(self.datafilename, "r") as f:
            alldata = f.readlines()
            for line in alldata:
                nums = list(map(int, filter(str.isdigit, line.split())))
                all_nums.extend(nums)
            maxnum = all_nums[self.rank]
            for i in range(self.rank + self.size, len(all_nums), self.size):
                cnt += 1
                if maxnum < all_nums[i]:
                    maxnum = all_nums[i]
        if self.rank == 0:
            self.res.append(maxnum)
            with self.cond:
                self.cond.notify_all()
        elif self.size != 1:
            self.send_data(
                self.no_rank0_client, {"data_type": "res", "payload": maxnum}
            )
            self.no_rank0_client.shutdown(socket.SHUT_RDWR)
            self.no_rank0_client.close()
            os._exit(0)


if __name__ == "__main__":
    cal = My_Cal_Node()
