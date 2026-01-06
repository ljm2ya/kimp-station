use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open("orders.db")?;
    
    println!("\n📊 Database Statistics:");
    println!("=" .repeat(50));
    
    let mut stmt = conn.prepare(
        "SELECT source, COUNT(*) as count, 
         datetime(MAX(timestamp)/1000, 'unixepoch') as latest 
         FROM orderbook_snapshots 
         GROUP BY source"
    )?;
    
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    
    for row in rows {
        let (source, count, latest) = row?;
        println!("\n🔹 Source: {}", source);
        println!("   Count: {}", count);
        println!("   Latest: {}", latest);
    }
    
    println!("\n" + &"=".repeat(50));
    Ok(())
}
