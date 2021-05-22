def iterates(deps_list, __myself_id_list=[], __depth=0):
    # 更新myself_id_list的id，让外部调用时不需要填入参数
    __result = []

    __myself_id_list = list(set(__myself_id_list))
    print('e',id(__myself_id_list))
    myself_id = id(deps_list)
    __myself_id_list.append(myself_id)
    for value in deps_list:
        child_id = id(value)

        if isinstance(value, list):
            __depth += 1
            if child_id in __myself_id_list:

                __result.append(('self%s' % __depth, __depth, child_id, myself_id))
            else:

                __result += iterates(value, __myself_id_list, __depth)


        else:
            __result.append((value, __depth, child_id, myself_id))
    # result = list(set(__result))
    result = __result
    return result


if __name__ == '__main__':
    a = ['1', 'x', ['2', 'x', '3']]
    b = ['b1', 's', 'x', 'b2', a]
    a[2][2] = b
    b[2] = a
    print(a)
    print(b)

    for i in iterates(a):
        print(i)
    print('*' * 100)

    for i in iterates(a):
        print(i)
    print('*' * 100)
    for i in iterates(a):
        print(i)
