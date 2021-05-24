import copy


def iterates(deps_list, __myself_id_list={}):
    result = []
    myself_id = id(deps_list)
    __myself_id_list = copy.deepcopy(__myself_id_list)
    depth = __myself_id_list.setdefault(myself_id, 0)
    for value in deps_list:
        child_id = id(value)
        if isinstance(value, list):
            if child_id in __myself_id_list.keys():
                result.append(('self%s' % depth, depth))
            else:

                __myself_id_list.setdefault(child_id, depth + 1)
                result += iterates(value, __myself_id_list)

        else:
            result.append((value, depth))

    return result


if __name__ == '__main__':
    a = ['1', 'x', ['2', 'x', '3', ['s', 't', ['v', 'a']]], 'y', 'z']
    b = ['b1', 's', 'x', 'b2', a]
    a[2][3][2][1] = a
    a[1] = a
    print(a)
    result = iterates(a)
    print('*' * 100)
    for i in result:
        print(i)
