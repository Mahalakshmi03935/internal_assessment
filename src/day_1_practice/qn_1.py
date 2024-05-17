def change_case(string):
    result = ""
    for char in string:
        if char.isupper():
            result += "_" + char.lower()
        else:
            result += char
    return result.lstrip("_")


string = "MahaLakshmi"
print(change_case(string))