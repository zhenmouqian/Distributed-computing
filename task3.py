import sys
import math


class My_Cal_Node:
    def __init__(self):
        self.rank = int(sys.argv[1])
        self.size = int(sys.argv[2])
        self.datafilename = str(sys.argv[4])

        if int(sys.argv[3]) == 1:
            self.task3_1()
        else:
            self.maxNum = int(sys.argv[5])
            self.task3_2()

    def isprime(self, a):
        if a <= 1:
            return False
        for i in range(2, int(math.sqrt(a)) + 1):
            if a % i == 0:
                return False
        return True

    def task3_1(self):
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
            print(maxnum, end="")

    def task3_2(self):
        all_nums = []
        with open(self.datafilename, "r") as f:
            alldata = f.readlines()
            for line in alldata:
                nums = list(map(int, filter(str.isdigit, line.split())))
                all_nums.extend(nums)
            cnt = 0
            for i in range(self.rank + self.size, len(all_nums), self.size):
                if self.isprime(i):
                    cnt += 1
            print(cnt, end="")


if __name__ == "__main__":
    cal = My_Cal_Node()
