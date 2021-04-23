for i in range(10):
    print(f'i--------------:{i}')
    for j in range(3,10):
        if j %5 == 0:
            continue
        print(f'j:{j}')

