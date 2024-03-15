use postgres::{Client, NoTls};
use std::collections::BTreeMap;

#[derive(Debug)]
enum Type {
    Bool,
    Int4,
    Int8,
    Text,
    ByteArray,
    JSON,
    Timestamp,
    Enum(Vec<String>),
    Array(Box<Type>),
}

#[derive(Debug)]
struct Column {
    pub name: String,
    pub value_type: Type,
    pub value_nullable: bool,
    pub value_default: bool,
    pub primary_key: bool,
    pub foreign_key: Option<(String, String)>,
}

#[derive(Debug, Default)]
struct Table {
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub column_names: Vec<String>,
}

#[derive(Debug, Default)]
struct Database {
    pub tables: BTreeMap<String, Table>,
    pub table_names: Vec<String>,
}

fn run() -> Result<(), postgres::Error> {
    let mut client = Client::connect(&std::env::args().skip(1).next().unwrap(), NoTls)?;

    client.batch_execute("
      CREATE TABLE IF NOT EXISTS person (
        id      SERIAL PRIMARY KEY,
        name    TEXT NOT NULL,
        data    BYTEA
      )"
    )?;

    let name = "Ferris";
    let data = None::<&[u8]>;
    client.execute(
        "INSERT INTO person (name, data) VALUES ($1, $2)",
        &[&name, &data],
    )?;

    for row in client.query("SELECT id, name, data FROM person", &[])? {
        let id: i32 = row.get(0);
        let name: &str = row.get(1);
        let data: Option<&[u8]> = row.get(2);

        println!("found person: {} {} {:?}", id, name, data);
    }

    let tables = client.query("select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and is_insertable_into = 'YES' and is_typed = 'NO' order by table_name", &[])
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<String>>();

//    println!("{:#?}", tables);

    let columns = client.query("select table_name, column_name, is_nullable, column_default, data_type, udt_name from information_schema.columns where table_schema = 'public' order by table_name, column_name", &[])
        .unwrap()
        .into_iter()
        .map(|row|
             (row.get::<_, String>(0),
              row.get::<_, String>(1),
              row.get::<_, String>(2),
              row.get::<_, Option<String>>(3),
              row.get::<_, String>(4),
              row.get::<_, String>(5)))
        .collect::<Vec<(String, String, String, Option<String>, String, String)>>();

//    println!("{:#?}", columns);

    let constraints = client.query("select constraint_name, table_constraints.constraint_type, key_column_usage.table_name, key_column_usage.column_name, constraint_column_usage.table_name, constraint_column_usage.column_name from information_schema.table_constraints join information_schema.key_column_usage using(constraint_name) join information_schema.constraint_column_usage using(constraint_name) where table_constraints.constraint_schema = 'public' and table_constraints.table_schema = 'public' and key_column_usage.constraint_schema = 'public' and key_column_usage.table_schema = 'public' and constraint_column_usage.constraint_schema = 'public' and constraint_column_usage.table_schema = 'public' order by constraint_name;", &[])
        .unwrap()
        .into_iter()
        .map(|row|
             (row.get::<_, String>(0),
              row.get::<_, String>(1),
              row.get::<_, String>(2),
              row.get::<_, String>(3),
              row.get::<_, String>(4),
              row.get::<_, String>(5)))
        .collect::<Vec<(String, String, String, String, String, String)>>();

//    println!("{:#?}", constraints);

    let type_values = client.query("select pg_type.typname, pg_enum.enumlabel from pg_type join pg_enum on pg_enum.enumtypid = pg_type.oid where pg_type.typtype = 'e' and pg_type.typcategory = 'E' order by pg_type.typname;
", &[])
        .unwrap()
        .into_iter()
        .map(|row|
             (row.get::<_, String>(0),
              row.get::<_, String>(1)))
        .collect::<Vec<(String, String)>>();

//    println!("{:#?}", type_values);

    let mut db = Database::default();
    for table in tables {
        let table_name = table;

        db.tables.insert(table_name.clone(), Table::default());
        db.table_names.push(table_name);
    }

    for column in columns {
        let table_name = column.0;
        let column_name = column.1;
        let is_nullable = column.2 == "YES";
        let has_default = column.3.is_some();
        let value_type = if column.4 == "USER-DEFINED" {
            let values = type_values.iter()
                .filter(|(typename, _)| typename == &column.5)
                .map(|(_, valuename)| valuename.clone())
                .collect::<Vec<String>>();
            Type::Enum(values)
        } else if column.4 == "ARRAY" {
            if column.5 == "_text" {
                Type::Array(Box::new(Type::Text))
            } else {panic!("Unexpected ARRAY type: {}", column.5)}
        } else {
            if column.5 == "bool" {Type::Bool}
            else if column.5 == "int4" {Type::Int4}
            else if column.5 == "int8" {Type::Int8}
            else if column.5 == "text" {Type::Text}
            else if column.5 == "varchar" {Type::Text}
            else if column.5 == "bytea" {Type::ByteArray}
            else if column.5 == "jsonb" {Type::JSON}
            else if column.5 == "timestamp" {Type::Timestamp}
            else if column.5 == "timestamptz" {Type::Timestamp}
            else {panic!("Unexpected type: {}", column.5)}
        };

        let primary_key = constraints.iter()
            .filter(|(_, _, table, column, _, _)| &table_name == table && &column_name == column)
            .any(|(_, constraint_type, _, _, _, _)| constraint_type == "PRIMARY KEY");

        let mut foreign_keys = constraints.iter()
            .filter(|(_, _, table, column, _, _)| &table_name == table && &column_name == column)
            .filter(|(_, constraint_type, _, _, _, _)| constraint_type == "FOREIGN KEY")
            .map(|(_, _, _, _, table, column)| (table.clone(), column.clone()))
            .collect::<Vec<(String, String)>>();

        if foreign_keys.len() > 1 {
            panic!("More than one foreign key information found for one column:\n{:#?}", constraints.iter()
                   .filter(|(_, _, table, column, _, _)| &table_name == table && &column_name == column)
                   .filter(|(_, constraint_type, _, _, _, _)| constraint_type == "FOREIGN KEY"));
        }

        let column = Column {
            name: column_name.clone(),
            value_type,
            value_nullable: is_nullable,
            value_default: has_default,
            primary_key,
            foreign_key: foreign_keys.pop()
        };

        if let Some(table) = db.tables.get_mut(&table_name) {
            (*table).columns.insert(column_name.clone(), column);
            (*table).column_names.push(column_name);
        } else {
            println!("Table {} not found for column {}", table_name, column_name);
        }
    }

    println!("{:#?}", db);

    Ok(())
}


fn main() {
    println!("Hello, world!");
    run().unwrap();
}
