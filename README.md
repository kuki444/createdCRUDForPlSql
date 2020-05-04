Create CRUD Data for PL/SQL
====

テーブルリストからPL/SQL文（ファイル）のCRUD(C:inset,R:select,U:update,D:delete)をExcelファイルに作成する。

## Description
crud.py [-l テーブルリスト] [-o 出力ファイル名] [-t (excel|csv)] <対象ファイル|対象フォルダ> ..

(対象ファイル|対象フォルダ)は、複数指摘出来ます。

-l:
テーブルリストを指定しない場合は、カレントフォルダにある「table_list.txt」を元にCRUDを作成します。
デフォルト：./table_list.txt
-o:
出力ファイル名を設定（拡張子が設定さてていない場合は、出力タイプに従い xlsx or csvが設定されます。
デフォルト：./result.xlsx or ./result.csv
-t:
出力されるファイルのタイプを設定します。Excelファイルもしくは、csvファイルを設定します。
デフォルト：excel

## Requirement
...
pip3 install pandas
pip3 install openpyxl
...

## Usage

crud.py ./filename.sql
crud.py ./testdir ./testdir2

crud.py -l ./listdir/table_list.txt ./testdir ./testdir2

sample run

 ./crud.py -l ./sample/table_list.txt ./sample/test ./sample/test2

## Install
```
 chmod +x crud.py
```

## Contribution

## Licence

[MIT](https://github.com/tcnksm/tool/blob/master/LICENCE)

## test runing
python3 -m unittest tests.test_crud

## TODO
テストケース作成
class化
インストール用Makefile作成
