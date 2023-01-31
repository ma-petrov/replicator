# from replicator import IncrementReplicator

# IncrementReplicator({
#     'src': {'conn': 'src', 'table': 'test_read', 'schema': 'public', 'row_id': 'rid', 'batch_size': 1000,},
#     'dst': {'conn': 'dst', 'table': 'test_read', 'schema': 'public', 'row_id': 'rid',},
# }).replicate()


def print_func(a, b):
    print(f'a = {a}, list = {b}')

def modify_lst(a, b):
    print(f'modify list no {a}')
    b.clear()
    b.extend(['new', 'list'])

for t in [(1, [1, 2, 3]), (2, [1, 2, 3]), (3, [1, 2, 3])]:
    modify_lst(*t)
    print_func(*t)