use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = std::fs::read_to_string("test/mysql/ngcommon/account/initialize.sql").unwrap();
    let dialect = MySqlDialect {};
    let stmts = Parser::parse_sql(&dialect, &sql).unwrap();

    for (i, stmt) in stmts.iter().enumerate() {
        match stmt {
            sqlparser::ast::Statement::CreateTable(ct) => {
                println!(
                    "Table {}: {} ({} cols, {} constraints)",
                    i + 1,
                    ct.name,
                    ct.columns.len(),
                    ct.constraints.len()
                );

                // Check for tricky features
                for col in &ct.columns {
                    let dt = format!("{:?}", col.data_type);
                    for opt in &col.options {
                        let opt_str = format!("{:?}", opt.option);
                        if opt_str.contains("DialectSpecific") || opt_str.contains("OnUpdate") {
                            println!(
                                "  → {}: special option: {}",
                                col.name.value,
                                opt_str.chars().take(80).collect::<String>()
                            );
                        }
                    }
                    // Check for tinyint(1)
                    if dt.contains("TinyInt(Some(") {
                        println!("  → {}: TINYINT with display width", col.name.value);
                    }
                }
            }
            _ => {}
        }
    }
}
