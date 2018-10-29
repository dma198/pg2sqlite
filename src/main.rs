/*
    Supported PostgreSQL column types:
    BOOL,"char",SMALLINT,SMALLSERIAL,INT,SERIAL,OID,BIGINT,BIGSERIAL,REAL,DOUBLE PRECISION,
    VARCHAR,CHAR(n),TEXT,CITEXT,NAME,
    TIME,DATE,TIMESTAMP     
    
*/
#[macro_use]
extern crate clap;
extern crate colored;
extern crate postgres;
extern crate rusqlite;
extern crate chrono;
//extern crate zip;


use std::path::Path;
use clap::{App, Arg};
use colored::*;
use postgres::*;
use chrono::{NaiveDateTime, NaiveDate,NaiveTime};

fn main() {
    let matches=App::new("pg2sqlite")
        .version("v1.0")
        .about("Data export utility PostgreSQL => SQLite")
        .arg(
            Arg::with_name("pgurl")
                .value_name("PG URL")
                .help("PostgreSQL Connection URL: postgres://[[user]:pass]@<host>/<db>")
                .required(true)
                .index(1)
                .takes_value(true),
        ).arg(
            Arg::with_name("sqlitefile")
                .value_name("sqlite file")
                .help("SQLite File Name")
                .required(true)
                .index(2)
                .takes_value(true),
        ).arg(
            Arg::with_name("tables")
                .value_name("tables")
                .help("Comma separated table names")
                .required(true)
                .index(3)
                .takes_value(true),
        ).arg(
            Arg::with_name("batchsize")
                .long("batchsize")
                .value_name("batchsize")
                .help("Max count of records in one SQLite transaction")
                .required(false)
                .default_value("10000")
                .takes_value(true),
        ).arg(
            Arg::with_name("indexes")
                .short("i")
                .long("indexes")
                .value_name("indexes")
                .help("Export indexes. Default is false")
                .required(false)
                .default_value("false")
                .takes_value(false),
        ).arg(
            Arg::with_name("compress")
                .short("c")
                .long("compress")
                .value_name("compress")
                .help("Compress destination file. Default is false")
                .required(false)
                .default_value("false")
                .takes_value(false),
        ).get_matches();

 
    let pg_url = matches.value_of("pgurl").unwrap();
    let sqlite_file = matches.value_of("sqlitefile").unwrap();
    let tables = matches.value_of("tables").unwrap().split(",");
    let batch_size = value_t!(matches, "batchsize", usize).unwrap_or(10000);
    let export_indexes = value_t!(matches, "indexes", bool).unwrap_or(false);
    let compress = value_t!(matches, "compress", bool).unwrap_or(false);

    println!("Connecting to PostgreSQL:");
    print!(" {} ...", pg_url);
    let pg_conn: postgres::Connection;
    match postgres::Connection::connect(pg_url.clone(), TlsMode::None) {
        Ok(con) => pg_conn = con,
        Err(err) => {
            println!();
            println!(" {}", err.to_string().red().bold());
            return;
        }
    };
    println!(" {}", "OK".green().bold());

    print!("Collecting metadata ... ");
    let mut t_defs: Vec<TableDef> = vec![];
    for tbl in tables {
        if !load_table_def(&pg_conn,tbl, &mut t_defs){
            println!("Table {} not found!", tbl.red().bold());
        }
    }
    println!(" {}", "OK".green().bold());

    if Path::new(sqlite_file).exists(){
        println!("File {} is already exists. It will be re-created.",sqlite_file.yellow());
        std::fs::remove_file(sqlite_file).unwrap();
    }

    print!("Opening SQLite file ... ");
    let sqlite_conn: rusqlite::Connection;
    match rusqlite::Connection::open(sqlite_file){
        Ok(con) => sqlite_conn = con,
        Err(err) => {
            println!();
            println!(" {}", err.to_string().red().bold());
            return;
        }
    };  
     println!("{}","OK".green().bold());  

    print!("SQLite Schema Generation ... ");
    generate_sqlite_schema(&sqlite_conn,&mut t_defs);
    println!("{}","Done".green().bold());

    print!("Exporting Data ... ");
    export_data(&pg_conn,&sqlite_conn,&mut t_defs,batch_size);
   
    if export_indexes{
        print!("Exporting Indexes ... ");
        
    }

    if compress{
        print!("Compressing ...");
        
    }
   
    println!("Done");

}

struct ColumnDef{
    c_name:String,
    c_type:String,
    //c_def_val:String,
    c_null:String
}

struct TableDef {
    name: String,
    column_defs: Vec<ColumnDef>
}

fn load_table_def(con: &postgres::Connection,tname: &str, t_defs: &mut Vec<TableDef>) -> bool {
    let mut found =false;
    for row in &con
        .query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=$1",&[&tname]).unwrap()
    {
        let name : String = row.get(0);
        let tdef=TableDef{
            name:name,
            column_defs:load_cols_def(&con,&tname)
        };
        found=true;
        t_defs.push(tdef);
    }
    if !found{
        return false;
    }
    return true;
}

fn load_cols_def(con: &postgres::Connection,tname: &str) -> Vec<ColumnDef> {
    let mut res : Vec<ColumnDef> = vec![];

    for row in &con
        .query("
        SELECT 
            b.nspname as schema_name,
            b.relname as table_name,
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
           CASE WHEN 
                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                 FROM pg_catalog.pg_attrdef d
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
                'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                              FROM pg_catalog.pg_attrdef d
                              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
            ELSE
                ''
            END as column_default_value,
            CASE WHEN a.attnotnull = true THEN 
                'NOT NULL'
            ELSE
                'NULL'
            END as column_not_null,
            a.attnum as attnum,
            e.max_attnum as max_attnum            
        FROM pg_catalog.pg_attribute a
            JOIN 
             (SELECT c.oid,
                n.nspname,
                c.relname
              FROM pg_catalog.pg_class c
                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname ~ ('^('||$1||')$')
                AND pg_catalog.pg_table_is_visible(c.oid)
              ORDER BY 2, 3) b
            ON a.attrelid = b.oid
            JOIN 
             (SELECT 
                  a.attrelid,
                  max(a.attnum) as max_attnum
              FROM pg_catalog.pg_attribute a
              WHERE a.attnum > 0 
                AND NOT a.attisdropped
              GROUP BY a.attrelid) e
            ON a.attrelid=e.attrelid
        WHERE  pg_catalog.format_type(a.atttypid, a.atttypmod) NOT IN ('oid','cid','xid','tid')    
        ",&[&tname]).unwrap(){
            res.push(ColumnDef{
                c_name:row.get(2),
                c_type:row.get(3),
                //c_def_val:row.get(4),
                c_null:row.get(5)                
            })
        }

    return res;
}

fn generate_sqlite_schema(con: &rusqlite::Connection,t_defs: &mut Vec<TableDef>){
    for tbl in t_defs{
        let mut sql : String = format!("CREATE TABLE {} (",tbl.name);
        for col in &tbl.column_defs{
            sql.push_str(&format!("{} {} {},",col.c_name,col.c_type,col.c_null));
        };
        sql.pop();sql.push(')'); // Replace trailing comma to closed brucket
        con.execute(&*sql,&[]).unwrap();
    }
}

enum SqlVal{
    String(Option<String>),
    Byte(Option<i8>),
    Small(Option<i16>),
    UInteger(Option<u32>),
    Integer(Option<i32>),
    Long(Option<i64>),
    Double(Option<f64>),
    Bool(Option<bool>),
    Date(Option<NaiveDate>),
    Time(Option<NaiveTime>),
    DateTime(Option<NaiveDateTime>)
}

fn map_postgres_row_to_sqlite_params<'a>(row : &postgres::rows::Row,
                                pvals  : &'a mut Vec<SqlVal>)->Vec<&'a rusqlite::types::ToSql>{
    
    macro_rules!  add_param{
        ($opt:expr,f32,$typ2:expr,$ok:ident) => {
            if !$ok {
                match $opt{    
                        Some(Ok(v)) => {
                        let rval0 : Option<f32> = v;
                        let rval : Option<f64>;
                        match rval0 {
                            None => rval=None,
                            Some(v) => {rval = Some(v as f64);}
                        }
                        let val : SqlVal = $typ2(rval);
                        pvals.push(val);
                        $ok=true;
                    },       
                    Some(Err(_))=> {},
                    None => {}
                }
            }
        };
        ($opt:expr,$typ1:ident,$typ2:expr,$ok:ident) => {
            if !$ok {
                match $opt{    
                    Some(Ok(v)) => {
                        let rval : Option<$typ1> = v;
                        let val : SqlVal = $typ2(rval);
                        pvals.push(val);
                        $ok=true;
                    },       
                    Some(Err(_))=> {},
                    None => {}
                }
            }
        }
    }

    let mut params : Vec<&'a rusqlite::types::ToSql> = vec![];
    for i in 0usize..row.len() {

        let mut ok=false;
        // Try set String Parameter
        add_param!(row.get_opt(i),String,SqlVal::String,ok);
        add_param!(row.get_opt(i),i8,SqlVal::Byte,ok);
        add_param!(row.get_opt(i),i16,SqlVal::Small,ok);
        add_param!(row.get_opt(i),i32,SqlVal::Integer,ok);
        add_param!(row.get_opt(i),u32,SqlVal::UInteger,ok);
        add_param!(row.get_opt(i),i64,SqlVal::Long,ok);
        add_param!(row.get_opt(i),f32,SqlVal::Double,ok);
        add_param!(row.get_opt(i),f64,SqlVal::Double,ok);
        add_param!(row.get_opt(i),bool,SqlVal::Bool,ok);
        add_param!(row.get_opt(i),NaiveDate,SqlVal::Date,ok);
        add_param!(row.get_opt(i),NaiveTime,SqlVal::Time,ok);
        add_param!(row.get_opt(i),NaiveDateTime,SqlVal::DateTime,ok);
        if !ok{
            println!("Unsupported column type");
        }
    }

    for i in 0usize..row.len() {
        match &pvals[i]{
            SqlVal::Bool(vv)=>params.push(vv),
            SqlVal::String(vv)=>params.push(vv),
            SqlVal::Byte(vv)=>params.push(vv),
            SqlVal::Small(vv)=>params.push(vv),
            SqlVal::Integer(vv)=>params.push(vv),
            SqlVal::UInteger(vv)=>params.push(vv),
            SqlVal::Long(vv)=>params.push(vv),
            SqlVal::Double(vv)=>params.push(vv),
            SqlVal::Date(vv)=>params.push(vv),
            SqlVal::Time(vv)=>params.push(vv),
            SqlVal::DateTime(vv)=>params.push(vv)
            //_ =>{}
        }
    }

    return params;
}

fn export_data(con_pg: &postgres::Connection,con_sqlite: &rusqlite::Connection,
               t_defs: &mut Vec<TableDef>,
               batch_size : usize){
    for tbl in t_defs{
        println!("Exporting table {} ... ",tbl.name);

        let mut sql : String = format!("SELECT * FROM {}",tbl.name);
        let mut sqli : String = format!("INSERT INTO {} VALUES(",tbl.name);
        for _ in &tbl.column_defs{
            sqli.push_str(&format!("{},","?"));
        }
        sqli.pop();sqli.push(')'); // Replace trailing comma to closed brucket
  
        let mut sti=con_sqlite.prepare(&*sqli).unwrap();

        con_sqlite.execute("BEGIN",&[]).unwrap();
        let mut n_exported=0usize;   

        const PRINT_BATCH_SIZE :usize = 1000;let mut prn = 0usize;
        let mut com = 0usize;
        
        for row in &con_pg.query(&*sql,&[]).unwrap(){
            let mut pvals : Vec<SqlVal>  = vec![]; // Holds parameter values
            let params = map_postgres_row_to_sqlite_params(&row,&mut pvals);
            sti.execute(&params).unwrap();
            n_exported+=1;
            prn+=1;com+=1;
            if prn==PRINT_BATCH_SIZE{
                print!("\r {} rows",n_exported);
                prn=0;
            }
            if com==batch_size{
                con_sqlite.execute("COMMIT",&[]).unwrap();
                con_sqlite.execute("BEGIN",&[]).unwrap();
                com=0;
            }
        }
        con_sqlite.execute("COMMIT",&[]).unwrap();
        print!("\rExported {} rows",n_exported);
        println!(); 
    }
}

/*
fn compress(filename: &str) -> zip::result::ZipResult<()>
{
    let path = std::path::Path::new(filename);
    let file = std::fs::File::create(&path).unwrap();

    let mut zip = zip::ZipWriter::new(file);

    let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("test/☃.txt", options)?;
    zip.write_all(b"Hello, World!\n")?;

    zip.start_file("test/lorem_ipsum.txt", FileOptions::default())?;
    zip.write_all(LOREM_IPSUM)?;

    zip.finish()?;
    Ok(())
}*/

