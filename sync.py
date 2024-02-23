import os
import platform
import datetime
import shutil

file_path_del = '\\' if platform.system() == 'Windows' else '/'

def print_file_first_line(file_path):
    with open(file_path, 'r') as file:
        print(file.readline())

def print_timestamp(path: str, indent='\t\t'):
    print(indent + str((datetime.datetime.fromtimestamp(os.path.getmtime(path))).replace(microsecond=0)))

def walk_dir(dir, prefix, item_list: list[(str,str)]):
    items = os.scandir(dir)
    for item in items:
        full_path = os.path.join(dir, item.name)
        relative_path = os.path.join(prefix, item.name)
        # print('\t' * relative_path.count(file_path_del) + relative_path, end='')
        # print_timestamp(full_path)
        if item.is_dir():
            item_list.append(('d', relative_path))
            item_list = walk_dir(full_path, relative_path, item_list)
        else:
            item_list.append(('-', relative_path))
    return item_list

dst_dir = r"data\dst"
src_dir = r"data\src"

src_items = walk_dir(src_dir, "", [])
for item in src_items:
    item_type = item[0]
    relative_path = item[1]
    src_path = os.path.join(src_dir, relative_path)
    print(item_type + ' ' + relative_path)
    dst_path = os.path.join(dst_dir, relative_path)
    if item_type == '-':
        if os.path.exists(dst_path):
            print("\t\t\tFile exists, skip")
        else:
            print("\t\t\tCopying file")
            shutil.copy2(src_path, dst_path)
    elif item_type == 'd':
        if os.path.exists(dst_path):
            print("\t\t\tDirectory exists, skip")
        else:
            print("\t\t\tCreating directory")
            os.makedirs(dst_path, exist_ok=True)
