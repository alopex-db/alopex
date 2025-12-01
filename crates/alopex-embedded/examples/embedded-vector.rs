//! Vector API をひととおり試すデモ。
//! - Database 初期化（インメモリ）
//! - upsert_vector でメタデータ付きベクトルを格納
//! - search_similar で Cosine / L2 / InnerProduct を検索

use alopex_embedded::{Database, Metric, SearchResult, TxnMode};

fn print_results(title: &str, results: &[SearchResult]) {
    println!("\n=== {title} ===");
    if results.is_empty() {
        println!("結果なし");
        return;
    }
    for (idx, row) in results.iter().enumerate() {
        let meta = String::from_utf8_lossy(&row.metadata);
        let key = String::from_utf8_lossy(&row.key);
        println!(
            "#{:02} key={key} score={:.4} metadata={meta}",
            idx + 1,
            row.score
        );
    }
}

fn main() {
    println!("AlopexDB Vector API デモ (Cosine / L2 / InnerProduct)");

    run_cosine_demo();
    run_l2_demo();
    run_inner_product_demo();
    println!("\nデモ完了。`cargo run --example embedded-vector` で実行できます。");
}

// Cosine 用のデモ（DBを分けて格納・検索）
fn run_cosine_demo() {
    let db = Database::new();
    {
        let mut txn = db
            .begin(TxnMode::ReadWrite)
            .expect("Cosine 用トランザクション作成に失敗");
        txn.upsert_vector(b"cos:red", "色=赤".as_bytes(), &[1.0, 0.1], Metric::Cosine)
            .expect("Cosine ベクトル登録に失敗");
        txn.upsert_vector(
            b"cos:green",
            "色=緑".as_bytes(),
            &[0.1, 1.0],
            Metric::Cosine,
        )
        .expect("Cosine ベクトル登録に失敗");
        txn.upsert_vector(
            b"cos:mix",
            "色=黄緑".as_bytes(),
            &[0.8, 0.4],
            Metric::Cosine,
        )
        .expect("Cosine ベクトル登録に失敗");
        txn.commit().expect("Cosine データのコミットに失敗");
    }
    {
        let mut txn = db
            .begin(TxnMode::ReadOnly)
            .expect("Cosine 用読み取りトランザクションに失敗");
        let results = txn
            .search_similar(&[0.9, 0.2], Metric::Cosine, 3, None)
            .expect("Cosine 類似検索に失敗");
        print_results("Cosine 類似検索 (色ベクトル)", &results);
    }
}

// L2 用のデモ（DBを分けて格納・検索）
fn run_l2_demo() {
    let db = Database::new();
    {
        let mut txn = db
            .begin(TxnMode::ReadWrite)
            .expect("L2 用トランザクション作成に失敗");
        txn.upsert_vector(b"l2:home", "座標=(0,0)".as_bytes(), &[0.0, 0.0], Metric::L2)
            .expect("L2 ベクトル登録に失敗");
        txn.upsert_vector(
            b"l2:work",
            "座標=(10,10)".as_bytes(),
            &[10.0, 10.0],
            Metric::L2,
        )
        .expect("L2 ベクトル登録に失敗");
        txn.upsert_vector(b"l2:park", "座標=(7,6)".as_bytes(), &[7.0, 6.0], Metric::L2)
            .expect("L2 ベクトル登録に失敗");
        txn.commit().expect("L2 データのコミットに失敗");
    }
    {
        let mut txn = db
            .begin(TxnMode::ReadOnly)
            .expect("L2 用読み取りトランザクションに失敗");
        let results = txn
            .search_similar(&[8.5, 7.5], Metric::L2, 3, None)
            .expect("L2 類似検索に失敗");
        print_results("L2 近傍検索 (座標)", &results);
    }
}

// 内積用のデモ（DBを分けて格納・検索）
fn run_inner_product_demo() {
    let db = Database::new();
    {
        let mut txn = db
            .begin(TxnMode::ReadWrite)
            .expect("InnerProduct 用トランザクション作成に失敗");
        txn.upsert_vector(
            b"ip:alpha",
            "重み=軽量サービス".as_bytes(),
            &[0.2, 0.3, 0.5],
            Metric::InnerProduct,
        )
        .expect("InnerProduct ベクトル登録に失敗");
        txn.upsert_vector(
            b"ip:beta",
            "重み=バランス".as_bytes(),
            &[0.3, 0.3, 0.4],
            Metric::InnerProduct,
        )
        .expect("InnerProduct ベクトル登録に失敗");
        txn.upsert_vector(
            b"ip:gamma",
            "重み=計算優先".as_bytes(),
            &[0.1, 0.4, 0.7],
            Metric::InnerProduct,
        )
        .expect("InnerProduct ベクトル登録に失敗");
        txn.commit().expect("InnerProduct データのコミットに失敗");
    }
    {
        let mut txn = db
            .begin(TxnMode::ReadOnly)
            .expect("InnerProduct 用読み取りトランザクションに失敗");
        let results = txn
            .search_similar(&[0.1, 0.4, 0.6], Metric::InnerProduct, 3, None)
            .expect("InnerProduct 類似検索に失敗");
        print_results("InnerProduct 類似検索 (重み付き)", &results);

        // フィルタ付き検索の例（gamma のみを対象にする）。
        let filter_keys = vec![b"ip:gamma".to_vec()];
        let filtered = txn
            .search_similar(
                &[0.1, 0.4, 0.6],
                Metric::InnerProduct,
                3,
                Some(&filter_keys),
            )
            .expect("InnerProduct フィルタ検索に失敗");
        print_results("InnerProduct フィルタ付き検索 (gamma)", &filtered);
    }
}
