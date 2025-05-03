def fun1(f0, f1, num):
    if num == 0:
        return f0
    if num == 1:
        return f1
    n0, n1, n2 = f0, f1, f0 + f1
    for i in range(2, num):
        n0, n1, n2 = n1, n2, (-1) ** (i + 1) * (n1 + n2)
    return n2


def fun2(lst: list):
    index = 0
    _max = -1
    if len(lst) < 2:
        return []
    while len(lst) != 2:
        _max = -1
        for i in range(0, len(lst) - 1):
            if lst[i] + lst[i + 1] > _max:
                index = i
                _max = lst[i] + lst[i + 1]
        temp = (lst[index] + lst[index + 1]) // 2
        lst[index] = temp
        lst.pop(index + 1)
        # lst[: index + 1].extend(lst[index + 2 :])
    return lst


def fun3(lst: list):
    # dp = [1] * len(lst)
    # for i in range(len(lst)):
    #     for j in range(0, i):
    #         if lst[i] > lst[j]:
    #             dp[i] += dp[j]
    # return sum(dp)
    n = len(lst)
    dp = [1] * n  # 每个元素本身是一个递增子序列

    for i in range(n):
        for j in range(i):
            if lst[j] <= lst[i]:
                dp[i] += dp[j]

    return sum(dp)  # 所有以不同位置结尾的递增子序列总和


print(fun2([2, 2, 3, 4, 5]))
# print(fun2([7, 0, 7, 8]))


# for i in range(0, 20):
#     print(fun1(1, 2, i))
# print(fun1(1, 1, 3))
