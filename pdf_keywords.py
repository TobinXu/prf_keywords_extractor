# -*- coding: UTF-8 -*-
# 2022-3-28 徐强国 经测试该代码配合data文件夹下的pdf以及与该代码同级文件夹下的target.xlsx和keywords.txt即可运行成功
# 其中keywords.txt用来写入想要获取的关键字


"""
1.加载一个指定路径文件夹内的所有pdf文内容
2.解析所有pdf内容并提取指定内容
3.把解析出来的指定内容写入Excel表格
"""
import os
import re
import sys
import time
from collections import defaultdict as ddict
from concurrent.futures import ProcessPoolExecutor, as_completed
try:
    import pdfplumber as ppb
    import pandas as pd
except:
    os.system('python -m pip install pdfplumber pandas -i https://pypi.tuna.tsinghua.edu.cn/simple')
    import pdfplumber as ppb
    import pandas as pd

__author__ = "xqg"
__email__ = "864339010@qq.com"
__version__ = "20220328 v1.0"


class ParsePDF:
    """关键词词频"""
    def __init__(self, file_dir, key_words_file, out_path):
        self.file_dir = file_dir
        self.pdf_files = self.load_pdf_files()
        self.key_words_file = key_words_file
        self.key_words = self.load_key_words()
        self.out_path = out_path

    def load_pdf_files(self):
        """读取一个文件夹目录下所有PDF文档路径,返回所有PDF文件的绝对路径"""
        pdf_files = []  # 保存文件地址和名称：name：path
        if not os.path.isdir(self.file_dir):
            print('无效的文件夹路径！')
            return pdf_files
        # pdf_files.append(['年份','股票代码','PDF名称'])
        for root, dirs, files in os.walk(self.file_dir):
            for file_ in files:
                if file_.endswith(('.pdf', '.PDF')):  # 判断是否为PDF文件
                    pdf_files.append(os.path.join(root, file_))
        print('所有的pdf', pdf_files)
        return pdf_files


    def load_key_words(self):
        """关键词"""
        if not os.path.isfile(self.key_words_file):
            print('无效的关键词文件地址！')
            return []
        with open(self.key_words_file, 'r', encoding='utf-8') as fin:
            words = fin.readlines()
        return [str(word).strip() for word in words if word and word !='']

    def task(self, file_):
        """解析"""
        print(f'进程{os.getpid()}: 正在解析文件：{file_}')
        words_count = ddict(int)
        try:
            text = '&page&'.join(str(page.extract_text()) for page in ppb.open(file_).pages)
            for key_word in self.key_words:
                words_count[key_word] += len(list(re.findall(f'{key_word}', text)))
        except Exception as e:
            print(f'pdfplumber parse file({os.path.basename(file_)}) failed! msg:{e}')
            pass
        return {file_: words_count}

    def run(self):
        print(f'一共{len(self.pdf_files)}个文件')
        # 提交任务多进程执行
        with ProcessPoolExecutor() as pool:
            tasks = [pool.submit(self.task, file_) for file_ in self.pdf_files]
      
        res_map = {}
        fail_counts = 0
        for res in as_completed(tasks):
            r = res.result()

            if r:
                res_map.update(r)
                print(f'文件解析完成！file:{list(r.keys())[0]}')
            else:
                fail_counts += 1
                print(f'文件解析失败! file:{list(r.keys()[0])}')
        print('获取的结果是', res_map)
        pd.DataFrame(res_map).T.to_excel(self.out_path)
        print(f'一共{fail_counts}个文件失败，{len(self.pdf_files)-fail_counts}个文件成功！结果保存到：{self.out_path}')


if __name__ == '__main__':
    """
    第一个参数：pdf文件夹地址
    第二个参数：关键词文件，一个关键词占一行
    第三个参数：保存地址
    """
    ParsePDF('./', 'keywords.txt', 'target.xlsx').run()