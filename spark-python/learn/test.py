if __name__ == '__main__':
    ldic = {}
    ldic = {1: 2, 3: 5}
    ldic.update({2: 9})

    print(ldic)
    sDic = sorted(ldic.items(), key=lambda kv: kv[1], reverse=True)
    print(sDic)
