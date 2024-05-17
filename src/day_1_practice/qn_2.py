lst = [3, 0, 1, 4, 6, 2, 7, 10]
n = 10
def find_missing_elements(lst, n):
    result = []
    for i in range(n + 1):
        if i not in lst:
            result.append(i)
    return result
missing_elements = find_missing_elements(lst, n)
print(missing_elements)