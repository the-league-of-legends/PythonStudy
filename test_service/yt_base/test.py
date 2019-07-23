# map_file = r'mysql_map\mysql_map'
# field_map = dict()
# with open(map_file) as mf:
#     for line in mf:
#         if not line:
#             continue
#         protocol, field_name, column_name = line.strip().upper().split('|')  # eg:['0001', 'ACKID', 'C0']
#         if protocol not in field_map:
#             field_map[protocol] = dict()
#
#         field_map[protocol][column_name] = field_name
#         print(field_map)

