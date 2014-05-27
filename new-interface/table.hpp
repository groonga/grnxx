#ifndef GRNXX_TABLE_HPP
#define GRNXX_TABLE_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Table {
 public:
  // 所属するデータベースを取得する．
  virtual DB *db() const = 0;
  // 名前を取得する．
  virtual const char *name() const = 0;
  // カラム ID の最小値を取得する．
  virtual ColumnID min_column_id() const = 0;
  // カラム ID の最大値を取得する．
  virtual ColumnID max_column_id() const = 0;
  // キーカラムを取得する．
  // キーカラムが存在しないときは nullptr を返す．
  virtual Column *key_column() const = 0;
  // 行 ID の最小値を取得する．
  virtual RowID min_row_id() const = 0;
  // 行 ID の最大値を取得する．
  virtual RowID max_row_id() const = 0;

  // カラムを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前がカラム名の条件を満たさない．
  // - 指定された名前のカラムが存在する．
  // - オプションの内容が不正である．
  // - 十分なリソースを確保できない．
  // - カラムの数が上限に達している．
  virtual Column *create_column(const char *column_name,
                                ColumnType column_type,
                                const ColumnOptions &column_options,
                                Error *error) = 0;
  // カラムを破棄する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 破棄するカラムを指す Column * を保持しているときは，
  // カラムを破棄する前に nullptr を代入するなどしておいた方がよい．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のカラムが存在しない．
  // - 依存関係を解決できない．
  virtual bool drop_column(const char *column_name, Error *error) = 0;

  // カラムの名前を変更する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のカラムが存在しない．
  // - 指定された名前（new）がカラム名の条件を満たさない．
  // - 指定された名前（new）のカラムが存在する．
  //  - 変更前後の名前が同じときは何もせずに成功とする．
  // - 索引の更新に失敗する．
  virtual bool rename_column(const char *column_name,
                             const char *new_column_name,
                             Error *error) = 0;

  // カラムの順番を変更する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // column_name で指定されたカラムを prev_column_name の後ろに移動する．
  // ただし， column_name と prev_column_name が同じときは移動せずに成功とする．
  // prev_column_name が nullptr もしくは空文字列のときは先頭に移動する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のカラムが存在しない．
  virtual bool reorder_column(const char *column_name,
                              const char *prev_column_name,
                              Error *error) = 0;

  // カラムを取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // min_column_id() 以上 max_column_id() 以下のカラム ID に対して
  // 呼び出すことですべてのカラムを取得することができる．
  // カラム ID は削除や並び替えによって変化することに注意が必要である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された ID が有効な範囲にない．
  virtual Column *get_column(ColumnID column_id， Error *error) const = 0;

  // カラムを検索する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された名前のカラムが存在しない．
  virtual Column *find_column(const char *column_name, Error *error) const = 0;

  // キーカラムを設定する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - キーカラムが存在している．
  // - 指定された名前のカラムが存在しない．
  // - 指定されたカラムの型がキーとしてサポートされていない．
  // - 指定されたカラムに同じ値が複数存在する．
  // - 一時領域を確保できない．
  virtual bool set_key_column(const char *column_name, Error *error) = 0;

  // キーカラムを解除する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - キーカラムが存在しない．
  virtual bool unset_key_column(Error *error) = 0;

  // 新しい行を追加する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 指定された行 ID が有効な場合，もしくは指定されたキーを持つ行が存在する場合，
  // その行 ID を *result_row_id に格納する．
  // 指定された行 ID が NULL_ROW_ID であれば，追加する行の ID は自動で選択する．
  // このとき，選択した行 ID を *result_row_id に格納する．
  // ただし，指定された行が存在する以外の理由で行の追加に失敗したときは
  // NULL_ROW_ID を *result_row_id に格納する．
  //
  // まとめると，返り値によって行の追加に成功したかどうかがわかり，
  // *result_row_id により該当する行が存在するかどうかがわかる．
  //
  // TODO: 行 ID の再利用を無効化するオプションがあると便利かもしれない．
  //       追加により割り当てられる行 ID は常に昇順という保証があれば，
  //       行 ID を降順に取り出すだけで新しい行から順に走査することができる．
  //       時刻カラムに対する索引で対応するより，ずっと効率的である．
  //       ただし，頻繁に削除すると隙間だらけになって効率が落ちる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された ID が行 ID の最小値より小さいか最大値 + 1 より大きい．
  // - キーカラムが存在しないとき
  //  - 指定された ID を持つ行が存在する．
  // - キーカラムが存在するとき
  //  - 指定された ID を持つ行が存在する．
  //  - 指定されたキーをキーカラムの型に変換できない．
  //  - 指定されたキーを持つ行が存在する．
  // - 行数が上限に達している．
  // - 索引の更新に失敗する．
  // - リソースを確保できない．
  virtual bool insert_row(RowID request_row_id,
                          const Datum &key,
                          RowID *result_row_id,
                          Error *error) = 0;

  // 行を削除する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 依存関係の解決が許されているときは，参照元を NULL にする．
  // 参照元が配列になっているときは，当該要素を取り除いて前方に詰める．
  //
  // TODO: 依存関係の解決に関する情報は，参照型のカラムに持たせることを検討中．
  //       SQL の ON DELETE + RESTRICT, CASCADE, SET NULL, NO ACTION に
  //       相当するが， NO ACTION は本当に何もしないのもありかもしれない．
  //
  // TODO: 参照元が配列のときは別に検討する必要がある．
  //       Groonga の場合は各配列から該当する参照を取り除いて前に詰める．
  //       位置によって意味が変わるような使い方では問題があるため，
  //       SET NULL も使えると嬉しいかもしれない．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された行が無効である．
  // - 指定された行は参照されている．
  //  - 依存関係を解決できるときは削除をおこなう．
  // - 依存関係の解決に失敗した．
  //
  // TODO: 不要になった（参照されなくなった）タグを削除するような用途を考えると，
  //       削除可能な行をすべて削除するという操作が実現できると便利そうである．
  //       デフラグに似た専用のインタフェースを提供すべきかもしれない．
  virtual bool delete_row(RowID row_id, Error *error) = 0;

  // 行の有効性を確認する．
  // 指定された行が有効であれば true を返す．
  // 指定された行が無効であれば false を返す．
  //
  // 更新する行を ID で指定されたときなどに用いる．
  //
  // 無効と判定される条件としては，以下のようなものが挙げられる．
  // - 指定された行 ID が有効範囲にない．
  // - 指定された行は削除されてから再利用されていない．
  virtual bool test_row(RowID row_id, Error *error) const = 0;

  // キーカラムを持つテーブルから行を検索する．
  // 成功すれば有効な行 ID を返す．
  // 失敗したときは *error にその内容を格納し， NULL_ROW_ID を返す．
  //
  // 更新する行をキーで指定されたときなどに使う．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - キーカラムが存在しない．
  // - 指定されたキーをキーカラムの型に変換できない．
  // - 指定されたキーを持つ行が存在しない．
  virtual RowID find_row(const Datum &key, Error *error) const = 0;

  // 行 ID を昇順もしくは降順に取得するためのカーソルを作成する．
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

  // 行 ID の一覧に適用できる式を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 索引による検索の後で絞り込むために用いる．
  // ほかに，スコアの調整や出力の指定，整列条件やグループ化条件の指定にも用いる．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // TODO: Expression のインタフェースを決める．
  //
  // TODO: 簡易なものでかまわないので，クエリ文字列をパースして式を構築してくれる
  //       インタフェースがあれば何かと便利かもしれない．
  //       テスト用と考えれば，最適化などは一切せず，
  //       真っ正直にやってくれるのがベスト．
  virtual std::unique_ptr<Expression> create_expression(
      const ExpressionOptions &options) const = 0;

  // 整列器を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // TODO: Sorter のインタフェースを決める．
  //
  // TODO: 索引，整列，グループ化の連携について検討する．
  //       たとえば，整列の途中経過をグループ化に利用するなど．
  //       行の整列とグループの整列が考えられる．
  virtual std::unique_ptr<Sorter> create_sorter(
      const SorterOptions &options) const = 0;

 protected:
  Table();
  virtual ~Table();
};

}  // namespace grnxx

#endif  // GRNXX_TABLE_HPP
