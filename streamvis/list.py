from tensorflow.io.gfile import GFile
from streamvis import util
import fire

def main(path):
    fh = GFile(path, 'rb')
    packed = fh.read()
    fh.close()
    messages = util.unpack(packed)
    groups, all_points = util.separate_messages(messages)
    # print(f'Inventory for {path}')
    print('id\tscope\tname\tindex\ttotal_vals')
    for g in groups:
        points = list(filter(lambda p: p.group_id == g.id, all_points))
        total_vals = sum(util.num_point_data(p) for p in points)
        print(f'{g.id}\t{g.scope}\t{g.name}\t{g.index}\t{total_vals}') 

if __name__ == '__main__':
    fire.Fire(main)

