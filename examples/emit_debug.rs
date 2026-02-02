use schema_conv::ast::Dialect;
use schema_conv::transpiler::SqlTranspiler;

fn main() {
    let transpiler = SqlTranspiler::new();

    let mysql = "CREATE TABLE users (
        id BIGINT NOT NULL AUTO_INCREMENT,
        email VARCHAR(255) NOT NULL,
        name VARCHAR(100),
        active BOOLEAN DEFAULT FALSE,
        balance DECIMAL(10,2) DEFAULT 0,
        metadata JSON,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE (email)
    );";

    println!("=== INPUT (MySQL) ===");
    println!("{}", mysql);

    let oracle = transpiler
        .convert(mysql, Dialect::MySQL, Dialect::Oracle)
        .unwrap();

    println!("=== OUTPUT (Oracle) ===");
    println!("{}", oracle);
}
