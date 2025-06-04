import sys


class My_Cal_Node:
    def __init__(self):
        self.rank = int(sys.argv[1])
        self.size = int(sys.argv[2])
        self.datafilename = str(sys.argv[4])
        self.task1_1()

    def task1_1(self):
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


if __name__ == "__main__":
    cal = My_Cal_Node()
