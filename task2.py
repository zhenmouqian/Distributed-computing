import os
import sys
import socket
import threading
import pickle
import struct
import json


class My_Cal_Node:
    def __init__(self):
        self.firstcond = threading.Condition()
        self.secondcond = threading.Condition()
        self.task2cond = threading.Condition()
        self.rank = int(sys.argv[1])
        self.size = int(sys.argv[2])
        self.datafilename = str(sys.argv[3])
        self.rank0ip: list = str(sys.argv[4])
        self.rank0port: list = int(sys.argv[5])
        self.maxNum = 0
        self.firstres = []
        self.secondres = []
        self.client_list_of_rank0_server = []

        if self.rank == 0:

            self.rank0_server: socket.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            )
            self.rank0_server.bind((self.rank0ip, self.rank0port))
            self.rank0_server.listen(5)
            threading.Thread(target=self.start_accept_no_rank0_client).start()
            threading.Thread(target=self.get_first_res).start()
            threading.Thread(target=self.get_final_res).start()
        else:
            self.no_rank0_client: socket.socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM
            )
            self.no_rank0_client.connect((self.rank0ip, self.rank0port))
            threading.Thread(target=self.message_handle_for_no_rank0).start()

        self.do_task2()

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
                    if data.get("data_type") == "res1":
                        self.firstres.append(int(data["payload"]))
                        with self.firstcond:
                            self.firstcond.notify_all()
                    elif data.get("data_type") == "res2":
                        self.secondres.extend((data["payload"]))
                        with self.secondcond:
                            self.secondcond.notify_all()
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

    def message_handle_for_no_rank0(self):
        """
        每个no_rank0节点处理来自rank0节点的消息
        for task2
        """
        while True:
            data = self.recv_data_for_no_rank0()
            if data:
                self.maxNum = data.get("payload")
                # no_rank0节点开始第二部分任务
                with self.task2cond:
                    self.task2cond.notify_all()
            else:
                break

    def recv_data_for_no_rank0(self):
        """
        每个no_rank0节点接收来自rank0节点的消息
        """
        raw_length = self.no_rank0_client.recv(4)
        if raw_length == b"":
            self.no_rank0_client.close()
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

    def get_first_res(self):
        with self.firstcond:
            while len(self.firstres) != self.size:
                self.firstcond.wait()
            for con in self.client_list_of_rank0_server:
                self.send_data(
                    con, {"data_type": "first_res", "payload": max(self.firstres)}
                )
            self.maxNum = max(self.firstres)
            # rank0节点开始第二部分任务
            with self.task2cond:
                self.task2cond.notify_all()

    def get_final_res(self):
        cnt = 0
        with self.secondcond:
            while cnt != self.size:
                cnt += 1
                self.secondcond.wait()
            self.secondres.sort(reverse=True)
            res = {"data_type": "res", "task": "task2", "res2": self.secondres[1]}
            print(json.dumps(res), flush=True, end="")
        os._exit(0)

    def send_data(self, con: socket.socket, data):
        socket_data = pickle.dumps(data)
        data_len = struct.pack("!I", len(socket_data))
        con.sendall(data_len + socket_data)

    def gcd(self, a, b):
        return a if b == 0 else self.gcd(b, a % b)

    def do_task2(self):
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
                self.firstres.append(maxnum)
                with self.firstcond:
                    self.firstcond.notify_all()
            else:
                self.send_data(
                    self.no_rank0_client, {"data_type": "res1", "payload": maxnum}
                )

            # wait first res
            with self.task2cond:
                self.task2cond.wait()
            if self.maxNum != -1:
                secondMaxPrimeNum = -2
                firstMaxPrimeNum = -1
                for i in range(self.rank + self.size, len(all_nums), self.size):
                    if all_nums[i] == self.maxNum:
                        continue
                    if self.gcd(self.maxNum, all_nums[i]) == 1:
                        if all_nums[i] > firstMaxPrimeNum:
                            secondMaxPrimeNum = firstMaxPrimeNum
                            firstMaxPrimeNum = all_nums[i]
                        elif all_nums[i] > secondMaxPrimeNum:
                            secondMaxPrimeNum = all_nums[i]

                # reslist.extend([firstMaxPrimeNum, secondMaxPrimeNum])

                if self.rank == 0:
                    self.secondres.extend([firstMaxPrimeNum, secondMaxPrimeNum])
                    with self.secondcond:
                        self.secondcond.notify_all()
                else:
                    self.send_data(
                        self.no_rank0_client,
                        {
                            "data_type": "res2",
                            "payload": [firstMaxPrimeNum, secondMaxPrimeNum],
                        },
                    )
                    self.no_rank0_client.shutdown(socket.SHUT_RDWR)
                    self.no_rank0_client.close()


if __name__ == "__main__":
    cal = My_Cal_Node()
