use postgres::{Client, NoTls};
use std::collections::{BTreeMap, BTreeSet};
use time::{OffsetDateTime};
use rand::Rng;
use rand::prelude::SliceRandom;
use postgres::types::{ToSql, to_sql_checked, FromSql};

#[derive(Debug)]
pub struct TypedString {
    pub value: String,
}

#[derive(Debug, Clone)]
enum Value {
    Int8(i64),
    Text(String),
}

impl ToSql for TypedString {
    fn to_sql(&self, ty: &postgres::types::Type, out: &mut postgres::types::private::BytesMut) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        self.value.to_sql(ty, out)
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl ToSql for Value {
    fn to_sql(&self, ty: &postgres::types::Type, out: &mut postgres::types::private::BytesMut) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Value::Int8(v) => v.to_sql(ty, out),
            Value::Text(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(ty: &postgres::types::Type) -> bool {
        ty.name() == "text" || ty.name() == "int8"
    }

    to_sql_checked!();
}

impl FromSql<'_> for Value {
    fn from_sql<'a>(ty: &postgres::types::Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if ty.name() == "text" {
            Ok(Value::Text(String::from_sql(ty, raw)?))
        }
        else if ty.name() == "int8" {
            Ok(Value::Int8(i64::from_sql(ty, raw)?))
        }
        else {
            panic!("unknown type: {}", ty.name());
        }
    }
    fn accepts(ty: &postgres::types::Type) -> bool {
        ty.name() == "text" || ty.name() == "int8"
    }
}

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

struct InsertInformation {
    table: String,
    data: Vec<(String, Option<(i32, Box<(dyn postgres::types::ToSql + Sync)>, Option<String>)>)>,
}

fn rand_int() -> i32 {
    let mut rng = rand::thread_rng();

    let v = match rng.gen_range(0..10) {
        0 => 0,
        1 => rng.gen_range(0..3),
        2 => rng.gen_range(0..10),
        3 => rng.gen_range(0..30),
        4 => rng.gen_range(0..100),
        5 => rng.gen_range(0..300),
        6 => rng.gen_range(0..1000),
        7 => rng.gen_range(0..10000),
        8 => rng.gen_range(0..100000),
        _ => rng.gen_range(0..1000000),
    };
    if 0 == rng.gen_range(0..10) {-v} else {v}
}

fn rand_str() -> String {
    use rand::distributions::DistString;
    let mut rng = rand::thread_rng();

    let len = rng.gen_range(0..50);

    match rng.gen_range(0..10) {
        0 => rng.sample_iter::<char, _>(rand::distributions::Standard)
            .take(len)
            .collect(),
        _ => rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), len),
    }
}

impl Database {
    fn insert_in_table(&self, client: &mut postgres::Transaction, table: &str, set_column: Option<(&str, Value)>, return_column: Option<&str>) -> Result<Option<Value>, postgres::Error> {
        let mut rng = rand::thread_rng();
        let mut data: Vec<(String, Option<(i32, Box<(dyn postgres::types::ToSql + Sync)>, Option<String>)>)> = Vec::new();

        let mut counter = 0;
        for column in &self.tables[table].column_names {
            println!("  {}", column);

            let column_info = &self.tables[table].columns[column];

            if set_column.is_some() && &set_column.as_ref().unwrap().0 == column {
                counter += 1;
                data.push((column.clone(), Some((counter, Box::new(set_column.as_ref().unwrap().1.clone()), None))));
            } else if column_info.value_nullable && 0 == rng.gen_range(0..3) {
                data.push((column.clone(), None));
            }
            else if column_info.value_default && 0 != rng.gen_range(0..3) {
                data.push((column.clone(), None));
            } else {
                if let Some((ftable, fcolumn)) = &column_info.foreign_key {
                    let count: i64 = client.query(&format!("select count(*) from {};", ftable), &[])
                        .unwrap()
                        .into_iter()
                        .map(|row| row.get::<_, i64>(0))
                        .next()
                        .unwrap();

                    let value: Box<(dyn postgres::types::ToSql + Sync)> =
                        match &column_info.value_type {
                            Type::Int8 => {
                                let id: i64 = client.query(&format!("select {} from {} limit 1 offset {};", fcolumn, ftable, rng.gen_range(0..count)), &[])
                                    .unwrap()
                                    .into_iter()
                                    .map(|row| row.get::<_, i64>(0))
                                    .next()
                                    .unwrap();
                                Box::new(id)
                            },
                            Type::Text => {
                                let id: String = client.query(&format!("select {} from {} limit 1 offset {};", fcolumn, ftable, rng.gen_range(0..count)), &[])
                                    .unwrap()
                                    .into_iter()
                                    .map(|row| row.get::<_, String>(0))
                                    .next()
                                    .unwrap();
                                Box::new(id)

                            },
                            _ => panic!("Foreign keys only supported of type int8 and text!"),
                        };

                    counter += 1;
                    data.push((column.clone(), Some((counter, value, None))));
                }
                else {
                    let value: Box<(dyn postgres::types::ToSql + Sync)> =
                    match &column_info.value_type {
                        Type::Bool => Box::new(0 == rng.gen_range(0..2)),
                        Type::Int4 => Box::new(rand_int() as i32),
                        Type::Int8 => Box::new(rand_int() as i64),
                        Type::Text => Box::new(rand_str()),
                        Type::ByteArray => Box::new(Vec::<u8>::new()),
                        Type::JSON => Box::new("{}"),
                        Type::Timestamp => Box::new(OffsetDateTime::now_utc()),
                        Type::Enum(values) => Box::new(TypedString {value: values.choose(&mut rng).unwrap().clone()}),
                        Type::Array(_) => Box::new(Vec::<String>::new()),
                    };

                    counter += 1;
                    data.push((column.clone(), Some((counter, value, if let Type::JSON = column_info.value_type {Some("JSON".to_string())} else {None}))));
                }
            }
        }

        let infos = InsertInformation {
            table: table.to_string(),
            data,
        };

        let column_names = infos.data.iter()
            .map(|(name, _)| format!("\"{}\"", name))
            .collect::<Vec<String>>();

        let column_ids = infos.data.iter()
            .map(|(_, idval)|
                 if let Some((id, _, Some(typespecifier))) = idval {format!("${}::typespecifier", id)}
                 else if let Some((id, _, None)) = idval {format!("${}", id)}
                 else {"DEFAULT".to_string()})
            .collect::<Vec<String>>();

        let column_vals = infos.data.into_iter()
            .filter_map(|(_, idval)| idval.map(|(_, val, _)| val))
            .collect::<Vec<Box<(dyn postgres::types::ToSql + Sync)>>>();

        let column_vals_refs = column_vals.iter()
            .map(Box::as_ref).collect::<Vec<&(dyn postgres::types::ToSql + Sync)>>();


        if let Some(return_column) = return_column {
            let insertion = format!("INSERT INTO \"{}\" ({}) VALUES ({}) RETURNING {}", infos.table, column_names.join(", "), column_ids.join(", "), return_column);

            println!("{}", insertion);
            println!("{:?}", &column_vals_refs[0..]);

            let res = client.query(
                &insertion,
                &column_vals_refs[0..],
            )?
                .into_iter()
                .map(|row| row.get::<_, Value>(0))
                .next()
                .unwrap();

            Ok(Some(res))
        }
        else {
            let insertion = format!("INSERT INTO \"{}\" ({}) VALUES ({})", infos.table, column_names.join(", "), column_ids.join(", "));

            println!("{}", insertion);
            println!("{:?}", &column_vals_refs[0..]);

            let res = client.execute(
                &insertion,
                &column_vals_refs[0..],
            )?;

            Ok(None)
        }
    }
}

fn collect_table_information(client: &mut postgres::Client) -> Result<Database, postgres::Error> {
    let tables = client.query("select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and is_insertable_into = 'YES' and is_typed = 'NO' order by table_name", &[])
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<String>>();

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

    let type_values = client.query("select pg_type.typname, pg_enum.enumlabel from pg_type join pg_enum on pg_enum.enumtypid = pg_type.oid where pg_type.typtype = 'e' and pg_type.typcategory = 'E' order by pg_type.typname;
", &[])
        .unwrap()
        .into_iter()
        .map(|row|
             (row.get::<_, String>(0),
              row.get::<_, String>(1)))
        .collect::<Vec<(String, String)>>();

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

    Ok(db)
}

struct Parameters {
    pub onlys: Vec<String>,
    pub skips: BTreeSet<String>,
    pub require_afters: BTreeMap<String, (String, String, String)>,
    pub require_befores: BTreeMap<String, (String, String, String)>,
}

fn parse_arguments() -> Parameters {
    let mut onlys = Vec::<String>::new();
    let mut skips = BTreeSet::<String>::new();
    let mut require_afters = BTreeMap::<String, (String, String, String)>::new();
    let mut require_befores = BTreeMap::<String, (String, String, String)>::new();

    for argument in std::env::args().skip(2) {
        if let Some(require_after_arguments) = argument.strip_prefix("--require-after=") {
            if let [table, column, atable, acolumn] = &require_after_arguments.split(",").collect::<Vec<&str>>()[..]  {
                require_afters.insert(table.to_string(), (column.to_string(), atable.to_string(), acolumn.to_string()));
            } else {
                panic!("Wrong arguments to --require-after=: Expecting 'table,column,aftertable,aftercolumn', got '{}'", require_after_arguments);
            }
        }
        else if let Some(require_before_arguments) = argument.strip_prefix("--require-before=") {
            if let [table, column, btable, bcolumn] = &require_before_arguments.split(",").collect::<Vec<&str>>()[..]  {
                require_befores.insert(table.to_string(), (column.to_string(), btable.to_string(), bcolumn.to_string()));
            } else {
                panic!("Wrong arguments to --require-before=: Expecting 'table,column,beforetable,beforecolumn', got '{}'", require_before_arguments);
            }
        }
        else if let Some(only_argument) = argument.strip_prefix("--only=") {
            onlys.push(only_argument.to_string());
        }
        else if let Some(skip_argument) = argument.strip_prefix("--skip=") {
            skips.insert(skip_argument.to_string());
        } else {
            if argument != "--help" {
                println!("Unknow parameter {}", argument);
            }
            panic!("Possible parameters are \n  --skip=table\n  --only=table\n  --require-after=table,column,atable,acolumn\n  --require-before=table,column,btable,bcolumn");
        }
    }

    if skips.len() > 0 && onlys.len() > 0 {
        panic!("Parameters '--only=' and '--skip' can not be combined.");
    }

    Parameters {
        onlys,
        skips,
        require_afters,
        require_befores,
    }
}

fn run() -> Result<(), postgres::Error> {
    let mut client = Client::connect(&std::env::args().skip(1).next().unwrap(), NoTls)?;

    let db = collect_table_information(&mut client)?;

    let mut rng = rand::thread_rng();
    let mut insertions = 0;

    let params = parse_arguments();

    loop {
        let random_table: &String = if params.onlys.len() > 0 {
            params.onlys.choose(&mut rng).unwrap()
        } else {
            db.table_names.choose(&mut rng).unwrap()
        };

        if params.skips.contains(random_table) {continue}

        println!("Creating new row for table: {}", random_table);

        let mut transaction = client.transaction()?;

        if let Some((rcolumn, atable, acolumn)) = params.require_afters.get(random_table) {
            let res = db.insert_in_table(&mut transaction, random_table, None, Some(rcolumn));

            match res {
                Ok(Some(value)) => {
                    insertions += 1;

                    let res = db.insert_in_table(&mut transaction, atable, Some((acolumn, value)), None);

                    match res {
                        Ok(_) => (),
                        Err(e) => println!("{}", e),
                    }
                },
                Ok(None) => panic!("Got no result!"),
                Err(e) => println!("{}", e),
            }
        } else if let Some((rcolumn, btable, bcolumn)) = params.require_befores.get(random_table) {
            let res = db.insert_in_table(&mut transaction, btable, None, Some(bcolumn));

            match res {
                Ok(Some(value)) => {
                    insertions += 1;

                    let res = db.insert_in_table(&mut transaction, random_table, Some((rcolumn, value)), None);

                    match res {
                        Ok(_) => (),
                        Err(e) => println!("{}", e),
                    }
                },
                Ok(None) => panic!("Got no result!"),
                Err(e) => println!("{}", e),
            }
        } else {
            let res = db.insert_in_table(&mut transaction, random_table, None, None);

            match res {
                Ok(_) => (),
                Err(e) => println!("{}", e),
            }
        }

        let res = transaction.commit();

        match res {
            Ok(_) => insertions += 1,
            Err(e) => println!("{}", e),
        }

        println!("{}", insertions);
    }
}

fn main() {
    run().unwrap();
}
