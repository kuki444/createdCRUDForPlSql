#!/usr/bin/python3

from concurrent import futures
import codecs
import tempfile
import os
import re
import glob
import sys
import time
import pandas as pd
import openpyxl
#pip3 install pandas
#pip3 install openpyxl

def main():

    # tempパス作成
    tmpdir = tempfile.TemporaryDirectory()
    # programe path
    pg_path = sys.argv[0]
    # current path
    current_path = os.path.dirname(pg_path)
    # table list path
    table_list_path = './table_list.txt'
    # src path
    src_paths = []
    for i in range(len(sys.argv)-1):
        src_paths.append(sys.argv[i+1])
    # result path
    result_path = os.path.join(current_path, 'result.csv')
    result_excle_path = os.path.join(current_path, 'result.xlsx')

    # crudデータ作成 並列処理
    future_list = []
    with futures.ThreadPoolExecutor(max_workers=16) as executor:
        for src_path in src_paths:
            for file_path in sorted(glob.glob(os.path.join(src_path,"*"))):
                # createdcrud呼び出し
                future = executor.submit(fn=createdcrud, arg_table_list_path=table_list_path, arg_tmpdir=tmpdir, arg_file_path=file_path)
                future_list.append(future)
            _ = futures.as_completed(fs=future_list)
    # join result files
    with open(result_path, 'wb') as saveFile:
        for file_path in sorted(glob.glob(os.path.join(tmpdir.name,"*_result.txt"))):
            data = open(file_path, "rb").read()
            saveFile.write(data)
            saveFile.flush()
    # result output Excel
    df = pd.read_csv(result_path, sep=',', header=None, names=['ファイル名','TABLE/VIEW名','c:insert','u:update','r:select','d:delete','t:type/rowType','c:createTable','d:dropTable'])
    df.to_excel(result_excle_path, sheet_name='crud_data', index=False)

    #tmpデータ削除
    tmpdir.cleanup()

    print('completed.')
    print('created CURD file' + result_path)

def createdcrud(arg_table_list_path, arg_tmpdir, arg_file_path):
    #tempパス
    tmpdir = arg_tmpdir
    # Shift_JIS ファイルのパス
    shiftjis_file_path = arg_file_path
    pg_name = os.path.splitext(os.path.basename(shiftjis_file_path))[0]
    file_name = os.path.basename(shiftjis_file_path)
    # UTF-8 ファイルのパス
    utf8_file_path = os.path.join(tmpdir.name, pg_name + 'utf8_data.txt')
    # UTF-8/comment off ファイルのパス
    utf8_comment_off_path = os.path.join(tmpdir.name, pg_name + 'utf8_comment_off_data.txt')
    # テーブルリスト
    table_list_path = arg_table_list_path
    # crudlist (result)
    result_path = os.path.join(tmpdir.name, pg_name + '_result.txt')
#    result_path = os.path.join('./', pg_name + '_result.txt')

    print('start created crud ...' + shiftjis_file_path)

    # 文字コードを utf-8/改行コードLFに変換して保存
    fin = codecs.open(shiftjis_file_path, "r", "shift_jis")
    fout_utf = codecs.open(utf8_file_path, "w", "utf-8")
    for row in fin:
        fout_utf.write(row.replace(('\r\n'),'\n'))
    fin.close()
    fout_utf.close()

    # コメントを削除して保存
    comment_on = 0
    fout_comment_off = open(utf8_comment_off_path, mode='w')
    with open(utf8_file_path,"r+t") as fin:
        for text in fin:
            text = text.rstrip()
            if comment_on == 0:
                if r'--' in text:
                    print(text[:text.find('--')], file=fout_comment_off)
                elif r'#' in text:
                    print(text[:text.find('#')], file=fout_comment_off)
                elif r'/*' in text:
                    if r'*/' in text:
                        fout_comment_off.write(text[:text.find('/*')])
                        print(text[text.find('*/')+2:], file=fout_comment_off)
                        comment_on = 0
                    else:
                        print(text[:text.find('/*')], file=fout_comment_off)
                        comment_on = 1
                else:
                    print(text, file=fout_comment_off)
            else:
                if r'*/' in text:
                    print(text[text.find('*/')+2:], file=fout_comment_off)
                    comment_on = 0
    fout_comment_off.close()

    # リスト検索・判定
    with open(table_list_path, mode='r+t') as fin:
        table_lists = fin.readlines()
    with open(utf8_comment_off_path, mode='r+t') as fin:
        findtext = fin.read().replace('\n', ' ')
    result_lists = open(result_path, mode='w')
    # 判定
    # テーブル・ビューリストをファイルから取込
    items = ''
    for item in table_lists:
        items = items + item.rstrip() + '|'
    items = items[:len(items)-1]
    # crud判定
    saleVal = re.compile('(SELECT|INSERT|UPDATE|DELETE|TRUNCATE|MERGE|CREATE +TABLE|DROP +TABLE|JOIN).*?(' + items + ')( |\;|\n)',flags=re.IGNORECASE)
    findresult = saleVal.findall(findtext)
    for finditems in findresult:
        if finditems:
            finditems = list(finditems)
            if re.search('SELECT', finditems[0], re.IGNORECASE):
                item_curd = ',1,,,,,'
            if re.search('INSERT', finditems[0], re.IGNORECASE):
                item_curd = '1,,,,,,'
            if re.search('UPDATE', finditems[0], re.IGNORECASE):
                item_curd = ',,1,,,,'
            if re.search('DELETE', finditems[0], re.IGNORECASE):
                item_curd = ',,,1,,,'
            if re.search('TRUNCATE', finditems[0], re.IGNORECASE):
                item_curd = ',,,1,,,'
            if re.search('MERGE', finditems[0], re.IGNORECASE):
                item_curd = '1,,,,,,'
                print(file_name + ',' + finditems[1].upper() + ',' + item_curd, file=result_lists)
                item_curd = ',,1,,,,'
            if re.search('CREATE', finditems[0], re.IGNORECASE):
                item_curd = ',,,,,1,'
            if re.search('DROP', finditems[0], re.IGNORECASE):
                item_curd = ',,,,,,1'
            if re.search('JOIN', finditems[0], re.IGNORECASE):
                item_curd = ',1,,,,,'
            print(file_name + ',' + finditems[1].upper() + ',' + item_curd, file=result_lists)
    # crud　type型判定
    saleVal = re.compile('(' + items + ')(\%|\..+\%TYPE)',flags=re.IGNORECASE)
    findresult = saleVal.findall(findtext)
    for finditems in findresult:
        if finditems:
            finditems = list(finditems)
            if re.search('%', finditems[1], re.IGNORECASE):
                item_curd = ',,,,1,,'
                print(file_name + ',' + finditems[0].upper() + ',' + item_curd, file=result_lists)
    result_lists.close()
    # 重複削除/集計/並び替え
    with open(result_path, mode='r+t') as fin:
        result_data = fin.readlines()
    result_data = set(result_data)
    result_lists = open(result_path, mode='w')
    result_lists.writelines(result_data)
    result_lists.flush()
    result_lists.close()

    df = pd.read_csv(result_path, sep=',', header=None, names=['pg','table','c:insert','u:update','r:select','d:delete','t:type/rowType','c:createTable','d:dropTable'])
    grouped = df.groupby(['pg','table'], as_index=False)
    grouped.sum().sort_values(['pg','table']).to_csv(result_path, sep=',', index=False, header=False)

    print('end created crud ...' + shiftjis_file_path)


if __name__ == '__main__':
    main()
