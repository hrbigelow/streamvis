import sys
from streamvis import util
from streamvis import data_pb2 as pb

def main(path: str, dest: str):

    index = util.Index.from_filters()
    index_file = util.index_file(path)
    with util.get_log_handle(index_file, "rb") as index_fh:
        index.update(index_fh)

    data_file = util.data_file(path)
    with util.get_log_handle(data_file, "rb") as data_fh:
        datas_map = util.load_data(data_fh, index.entry_list) 
        configs_map = util.load_data(data_fh, index.config_entry_list)

    offset = 0
    packs = []

    for entry_id, configs in configs_map.items():
        pack = b''.join(util.pack_message(config) for config in configs)
        entry = index.config_entries[entry_id]
        entry.beg_offset = offset
        entry.end_offset = offset + len(pack)
        offset += len(pack)
        packs.append(pack)

    for entry_id, datas in datas_map.items():
        pack = b''.join(util.pack_message(data) for data in datas)
        entry = index.entries[entry_id]
        entry.beg_offset=offset
        entry.end_offset=offset + len(pack)
        offset += len(pack)
        packs.append(pack)


    index_file = util.index_file(dest)
    with util.get_log_handle(index_file, "wb") as fh:
        index_bytes = index.to_bytes()
        util.safe_write(fh, index_bytes)

    data_file = util.data_file(dest)
    data_pack = b''.join(packs)
    with util.get_log_handle(data_file, "wb") as fh:
        util.safe_write(fh, data_pack)

if __name__ == "__main__":
    path, dest = sys.argv[1:3]
    main(path, dest)
