#ifndef GRNXX_DB_HPP
#define GRNXX_DB_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class DB {
 public:
  DB();
  virtual ~DB();

  // テーブル ID の最小値を取得する．
  virtual TableID min_table_id() const = 0;
  // テーブル ID の最大値を取得する．
  virtual TableID max_table_id() const = 0;

  // テーブルを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前がテーブル名の条件を満たさない．
  // - 指定された名前のテーブルが存在する．
  // - オプションの内容が不正である．
  // - 十分なリソースを確保できない．
  // - テーブルの数が上限に達している．
  virtual Table *create_table(const char *table_name,
                              const TableOptions &table_options,
                              Error *error) = 0;

  // テーブルを破棄する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 破棄するテーブルを指す Table * を保持しているときは，
  // テーブルを破棄する前に nullptr を代入するなどしておいた方がよい．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のテーブルが存在しない．
  // - 依存関係を解決できない．
  virtual bool drop_table(const char *table_name,
                          Error *error) = 0;

  // テーブルの名前を変更する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のテーブルが存在しない．
  // - 指定された名前（new）がテーブル名の条件を満たさない．
  // - 指定された名前（new）のテーブルが存在する．
  //  - 変更前後の名前が同じときは何もせずに成功とする．
  // - 索引の更新に失敗する．
  virtual bool rename_table(const char *table_name,
                            const char *new_table_name,
                            Error *error) = 0;

  // テーブルの順番を変更する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // table_name で指定されたテーブルを prev_table_name の後ろに移動する．
  // ただし， table_name と prev_table_name が同じときは移動せずに成功とする．
  // prev_table_name が nullptr もしくは空文字列のときは先頭に移動する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のテーブルが存在しない．
  virtual bool reorder_table(const char *table_name,
                             const char *prev_table_name,
                             Error *error) = 0;

  // テーブルを取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // min_table_id() 以上 max_table_id() 以下のテーブル ID に対して
  // 呼び出すことですべてのテーブルを取得することができる．
  // テーブル ID は削除や並び替えによって変化することに注意が必要である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された ID が有効な範囲にない．
  virtual Table *get_table(TableID table_id，
                           Error *error) const = 0;

  // テーブルを検索する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のテーブルが存在しない．
  virtual Table *find_table(const char *table_name, Error *error) const = 0;

  // データベースの内容をファイルに出力する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // path が nullptr もしくは空文字列のときは関連付けられているパスを用いる．
  //
  // 上書き保存では，一時ファイルを作成してから名前を変更する．
  // 別名で保存するときは，共有メモリの内容も保存する必要がある．
  // 環境によっては Copy-on-Write なファイル・コピーが使えるかもしれない．
  //
  // ファイルへの保存という機会を利用して隙間を埋めることができる．
  // 最適化の有無や圧縮方法などはオプションで指定するものとする．
  //
  // 共有メモリの内容をフラッシュするだけというオプションも考えられる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のファイルに到達できない．
  //  - ディレクトリを作成するオプションがあると便利かもしれない．
  // - 指定された名前のファイルが存在する．
  //  - 上書きを許可するオプションは欲しい．
  // - 指定された名前のファイルに対するアクセス権限がない．
  // - 作業領域が確保できない．
  // - ディスクの空き容量が足りない．
  virtual bool save(const char *path,
                    const DBSaveOptions &options,
                    Error *error) const = 0;
};

// データベースを開く，もしくは作成する．
// 成功すれば有効なオブジェクトへのポインタを返す．
// 失敗したときは *error にその内容を格納し， nullptr を返す．
//
// path が nullptr もしくは空文字列のときは一時データベースを作成する．
//
// 返り値は std::unique_ptr なので自動的に delete される．
// 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
//
// 失敗する状況としては，以下のようなものが挙げられる．
// - データベースを開くように指示されたのに，指定された名前のファイルが存在しない．
// - データベースを作成するように指示されたのに，指定された名前のファイルが存在する．
// - 指定された名前のファイルに到達できない．
//  - ディレクトリを作成するオプションがあると便利かもしれない．
// - 指定された名前のファイルに対するアクセス権限がない．
// - 指定された名前のファイルがデータベースのファイルではない．
// - データベースを構成するファイルが存在しない．
std::unique_ptr<DB> open_db(const char *path,
                            const OpenDBOptions &options,
                            Error *error);

// データベースを削除する．
// 成功すれば true を返す．
// 失敗したときは *error にその内容を格納し， false を返す．
//
// 失敗する状況としては，以下のようなものが挙げられる．
// - 指定された名前のファイルが存在しない．
// - 指定された名前のファイルに対するアクセス権限がない．
// - 指定された名前のファイルがデータベースのファイルではない．
// - データベースを構成するファイルが存在しない．
//  - 一部のファイルが欠けていても強制的に残りを削除するオプションは欲しい．
//  - データベースを開かずにパスのみから推論して削除したいケースもありうる．
// - ファイルの削除に失敗する．
bool drop_db(const char *path, const DropDBOptions &options, Error *error);

}  // namespace grnxx

#endif  // GRNXX_DB_HPP
