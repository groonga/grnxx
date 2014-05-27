#ifndef GRNXX_COLUMN_HPP
#define GRNXX_COLUMN_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Column {
 public:
  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;
  // 名前を取得する．
  virtual const char *name() const = 0;
  // カラムの種類を取得する．
  virtual ColumnType type() const = 0;
  // 索引 ID の最小値を取得する．
  virtual IndexID min_index_id() const = 0;
  // 索引 ID の最大値を取得する．
  virtual IndexID max_index_id() const = 0;
  // キーカラムかどうかを取得する．
  virtual bool is_key() const = 0;

  // 索引を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前が索引名の条件を満たさない．
  // - 指定された名前の索引が存在する．
  // - オプションの内容が不正である．
  // - 十分なリソースを確保できない．
  // - 索引の数が上限に達している．
  virtual Index *create_index(const char *index_name,
                              IndexType index_type,
                              const IndexOptions &options,
                              Error *error) = 0;
  // 索引を破棄する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 破棄する索引を指す Index * を保持しているときは，
  // 索引を破棄する前に nullptr を代入するなどしておいた方がよい．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前の索引が存在しない．
  virtual bool drop_index(const char *index_name, Error *error) = 0;

  // 索引の名前を変更する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前の索引が存在しない．
  // - 指定された名前（new）が索引名の条件を満たさない．
  // - 指定された名前（new）の索引が存在する．
  //  - 変更前後の名前が同じときは何もせずに成功とする．
  virtual bool rename_index(const char *index_name,
                            const char *new_index_name,
                            Error *error) = 0;

  // 索引の順番を変更する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // index_name で指定された索引を prev_index_name の後ろに移動する．
  // ただし， index_name と prev_index_name が同じときは移動せずに成功とする．
  // prev_index_name が nullptr もしくは空文字列のときは先頭に移動する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前の索引が存在しない．
  virtual bool reorder_index(const char *index_name,
                             const char *prev_index_name,
                             Error *error) = 0;

  // 索引を取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // min_index_id() 以上 max_index_id() 以下の索引 ID に対して
  // 呼び出すことですべての索引を取得することができる．
  // 索引 ID は削除や並び替えによって変化することに注意が必要である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された ID が有効な範囲にない．
  virtual Index *get_index(IndexID index_id, Error *error) const = 0;

  // 索引を検索する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前の索引が存在しない．
  virtual Index *find_index(const char *index_name, Error *error) const = 0;

  // TODO: 機能から索引を検索する API が欲しい．
  //       たとえば，範囲検索に使える索引を探す，全文検索に使える索引を探すなど．
  //       索引の種類（TREE_INDEX, HASH_INDEX, etc.）による検索で十分かもしれない．

  // 値を格納する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 更新しようとしている値と指定された値が同じであれば，何もせずに成功とする．
  // キーカラムでは，同じ値が存在しないか確認する．
  // 索引があれば，それらも更新する．
  // 参照型のカラムでは，あらかじめ行 ID にしておく必要がある．
  //
  // TODO: 参照先のキーで入力できると便利だが，そうすると ID 指定による
  //       入力はできなくなってしまう．
  //       何か良い解決法がないか検討する．
  //
  // TODO: Datum が行 ID と整数を別の型として扱えないか検討する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された行 ID が有効でない．
  // - 指定された値をカラムの型に変換できない．
  // - 指定された値がカラムの制約にかかる．
  // - リソースが確保できない．
  // - 索引の更新に失敗する．
  virtual bool set(RowID row_id, const Datum &datum, Error *error) = 0;

  // 値を取得する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 取得した値は *datum に格納する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された行 ID が有効でない．
  virtual bool get(RowID row_id, Datum *datum, Error *error) const = 0;

  // 指定された条件を持たす行の ID を取得するためのカーソルを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - オプションが不正である．
  // - リソースが確保できない．
  virtual std::unique_ptr<Cursor> create_cursor(
      const CursorOptions &options) const = 0;

 protected:
  Column();
  virtual ~Column();
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_HPP
