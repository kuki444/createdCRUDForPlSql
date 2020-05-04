#!/usr/bin/python3

import argparse
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

def main(table_list_path, output_file_path, output_file_type, src_paths):
    # tempパス作成
    tmpdir = tempfile.TemporaryDirectory()
    # result temp path
    result_path = os.path.join(tmpdir.name, 'result.csv')

    print('Start Created CRUD File')

    # crudデータ作成 並列処理
    future_list = []
    with futures.ThreadPoolExecutor(max_workers=16) as executor:
        for src_path in src_paths:
            for file_path in sorted(glob.glob(os.path.join(src_path,"*"))):
                # createdcrud呼び出し
                future = executor.submit(fn=createdcrud, arg_table_list_path=table_list_path.name, arg_tmpdir=tmpdir, arg_file_path=file_path)
                future_list.append(future)
            _ = futures.as_completed(fs=future_list)
    # join result files
    with open(result_path, 'wb') as saveFile:
        for file_path in sorted(glob.glob(os.path.join(tmpdir.name,"*_result.txt"))):
            data = open(file_path, "rb").read()
            saveFile.write(data)
            saveFile.flush()
    # result output file
    df = pd.read_csv(result_path, sep=',', header=None, names=['ファイル名','TABLE/VIEW名','c:insert','u:update','r:select','d:delete','t:type/rowType','c:createTable','d:dropTable'])
    df['CRUD'] = df[['c:insert','u:update','r:select','d:delete']].apply(chenge_crud, axis=1)
    df = df[['ファイル名','TABLE/VIEW名','CRUD','c:insert','u:update','r:select','d:delete','t:type/rowType','c:createTable','d:dropTable']]
    if output_file_type == 'excel':
        df.to_excel(output_file_path.name, sheet_name='crud_data', index=False)
    else:
        df.to_csv(output_file_path.name, index=False)

    #tmpデータ削除
    tmpdir.cleanup()

    print('End Created CRUD File.')
    print('Completed.')
    print('Created CURD File' + output_file_path.name)

def chenge_crud(crud):
    str_crud = ''
    if crud[0] > 0:
        str_crud = str_crud + 'C'
    else:
        str_crud = str_crud + ' '
    if crud[1] > 0:
        str_crud = str_crud + 'R'
    else:
        str_crud = str_crud + ' '
    if crud[2] > 0:
        str_crud = str_crud + 'U'
    else:
        str_crud = str_crud + ' '
    if crud[3] > 0:
        str_crud = str_crud + 'D'
    else:
        str_crud = str_crud + ' '
    return str_crud

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

    print('start created crud ...' + shiftjis_file_path)

    # 文字コードを utf-8/改行コードLFに変換して保存
    chengeCharacterCodeShiftJISToUTF8(shiftjis_file_path,utf8_file_path)

    # コメントを削除して保存
    deleteComment(utf8_file_path, utf8_comment_off_path)

    # リスト取得
    with open(table_list_path, mode='r+t') as fin:
        table_lists = fin.readlines()
    # 判定用ファイル内容取得
    with open(utf8_comment_off_path, mode='r+t') as fin:
        find_text = fin.read()
    
    # 判定
    # crud判定
    items_crud = judgmentCrud(find_text,table_lists)
    # crud判定結果出力
    result_lists = open(result_path, mode='w')
    for item_crud in items_crud:
        print(file_name + ',' + item_crud[0] + ',' + item_crud[1], file=result_lists)
    result_lists.close()

    # 集計/並び替え
    df = pd.read_csv(result_path, sep=',', header=None, names=['pg','table','c','u','r','d','type','createTable','dropTable'])
    grouped = df.groupby(['pg','table'], as_index=False)
    grouped = grouped.sum().sort_values(['pg','table'])
    grouped.to_csv(result_path, sep=',', index=False, header=False)

    print('end created crud ...' + shiftjis_file_path)

# コメントを削除して保存
def deleteComment(utf8_file_path, utf8_comment_off_path):
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

def chengeCharacterCodeShiftJISToUTF8(shiftjis_file_path, utf8_file_path):
    # 文字コードを utf-8/改行コードLFに変換して保存
    fin = codecs.open(shiftjis_file_path, "r", "shift_jis")
    fout_utf = codecs.open(utf8_file_path, "w", "utf-8")
    for row in fin:
        fout_utf.write(row.replace(('\r\n'),'\n'))
    fin.close()
    fout_utf.close()

def judgmentCrud(text, tablelists):
    # 検索用データから改行を除く
    findtext = text.replace('\n', ' ')
    # テーブル・ビューリストをファイルから取込
    items = ''
    for item in tablelists:
        items = items + item.rstrip() + '|'
    items = items[:len(items)-1]
    items_crud = list()
    saleVal = re.compile('(SELECT|INSERT|UPDATE|DELETE|TRUNCATE|MERGE|CREATE +TABLE|DROP +TABLE|JOIN).*?(' + items + ')( |;|\n)',flags=re.IGNORECASE)
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
                items_crud.append((finditems[1].upper(),item_curd))
                item_curd = ',,1,,,,'
            if re.search('CREATE', finditems[0], re.IGNORECASE):
                item_curd = ',,,,,1,'
            if re.search('DROP', finditems[0], re.IGNORECASE):
                item_curd = ',,,,,,1'
            if re.search('JOIN', finditems[0], re.IGNORECASE):
                item_curd = ',1,,,,,'
            items_crud.append((finditems[1].upper(),item_curd))
    # crud　type型判定
    saleValtype = re.compile('(' + items + ')(%|..+%TYPE)',flags=re.IGNORECASE)
    findresult = saleValtype.findall(findtext)
    for finditems in findresult:
        if finditems:
            finditems = list(finditems)
            if re.search('%', finditems[1], re.IGNORECASE):
                item_curd = ',,,,1,,'
                items_crud.append((finditems[0].upper(),item_curd))
    # 重複削除
    items_crud = list(set(items_crud))
    return items_crud

class FileTypeWithCheck(argparse.FileType):
    def __call__(self, string):
        if string and "w" in self._mode:
            if os.path.exists(string):
                print('File: exists. Is it OK to overwrite? [y/n] : ')
                ans = sys.stdin.readline().rstrip()
                ypttrn = re.compile(r'^y(es)?$', re.I)
                m = ypttrn.match(ans)
                if not m:
                    sys.stderr.write("Stop file overwriting.\n")
                    sys.exit(1)
                    # raise ValueError('Stop file overwriting')
            if os.path.dirname(string):
                os.makedirs(os.path.dirname(string),
                            exist_ok=True)
        return super(FileTypeWithCheck, self).__call__(string)

    def __repr__(self):
        return super(FileTypeWithCheck, self).__repr__()

def FileDirTypeWithExist(string):
    print(string)
    if os.path.exists(string) == False:
        msg = "%r is not File or Dir" % string
        raise argparse.ArgumentTypeError(msg)
    return string

if __name__ == '__main__':
    # オプション・引数チェック
    # 1.ArgumentParserオブジェクトを生成する
    parser = argparse.ArgumentParser(description='Created CRUD Script')
    # 2.ArgumentParserオブジェクトにパラメータ(引数)を追加していく
    parser.add_argument('-l', '--list', dest='table_list_path', help='テーブルリストファイル名', default=os.path.join(os.path.dirname(sys.argv[0]), 'table_list.txt'), type=FileTypeWithCheck('rb'))
    parser.add_argument('-o', '--outfile', help='出力ファイル名', default=os.path.join(os.path.dirname(sys.argv[0]), 'result.xlsx'), type=FileTypeWithCheck('wb'))
    parser.add_argument('-t', '--type', help='出力ファイルのタイプ設定（excel|csv）', choices=['excel','csv'], default='excel')
    #parser.add_argument('inputFilePath', help='解析ファイルもしくは、解析ファイルのあるフォルダを設定する', action='append', nargs='+')
    parser.add_argument('inputFilePaths', help='解析ファイルもしくは、解析ファイルのあるフォルダを設定する', nargs='+', type=FileDirTypeWithExist)
    # 3.ArgumentParserオブジェクトを使って起動パラメータを解析する
    args = parser.parse_args()

    # main処理
    main(args.table_list_path, args.outfile, args.type, args.inputFilePaths)
