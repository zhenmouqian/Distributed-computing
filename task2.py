import sys


class My_Cal_Node:
    def __init__(self):
        self.rank = int(sys.argv[1])
        self.size = int(sys.argv[2])
        self.datafilename = str(sys.argv[4])

        if int(sys.argv[3]) == 1:
            self.task2_1()
        else:
            self.maxNum = int(sys.argv[5])
            self.task2_2()

    def gcd(self, a, b):
        return a if b == 0 else self.gcd(b, a % b)

    def task2_1(self):
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

    def task2_2(self):
        all_nums = []
        with open(self.datafilename, "r") as f:
            alldata = f.readlines()
            for line in alldata:
                nums = list(map(int, filter(str.isdigit, line.split())))
                all_nums.extend(nums)
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
            print([firstMaxPrimeNum, secondMaxPrimeNum], end="")


if __name__ == "__main__":
    cal = My_Cal_Node()
