"""把项目的所有 .py 文件编译成 .pyc 文件，放入当前目录下的 dist 文件夹中。"""

import os
import shutil
import py_compile


def clear_dir(name):
    for root, dirs, files in os.walk(name):
        for name in files:  # 删除文件
            os.remove(os.path.join(root, name))
        for name in dirs:   # 删除目录
            shutil.rmtree(os.path.join(root, name))


def complie_all(path):
    dist = os.path.join(path, 'dist')
    if not os.path.exists(dist):
        os.mkdir(dist)
    else:
        clear_dir(dist)

    for dirpath, dirnames, filenames in os.walk(path):
        if dirpath == '.':
            compiler_file_name = os.path.split(__file__)[-1]
            for filename in filenames:
                if filename.endswith('.py') and filename != compiler_file_name:
                    src_file = os.path.join(dirpath, filename)
                    dst_file = os.path.join(dist, filename + 'c')
                    print(f'{src_file} -> {dst_file}')
                    py_compile.compile(src_file, cfile=dst_file)

        elif '__init__.py' in filenames:  # 这个目录是一个包
            path_name = dist + dirpath[1:]
            os.mkdir(path_name)
            for filename in filenames:
                src_file = os.path.join(dirpath, filename)
                dst_file = os.path.join(path_name, filename + 'c')
                print(f'{src_file} -> {dst_file}')
                py_compile.compile(src_file, cfile=dst_file)


if __name__ == '__main__':
    complie_all('.')
