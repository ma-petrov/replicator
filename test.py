from replicator import IncrementReplicator

IncrementReplicator({
    'src': {'conn': 'src', 'table': 'test_read', 'schema': 'public', 'row_id': 'rid', 'batch_size': 1000,},
    'dst': {'conn': 'dst', 'table': 'test_read', 'schema': 'public', 'row_id': 'rid',},
}).replicate()
