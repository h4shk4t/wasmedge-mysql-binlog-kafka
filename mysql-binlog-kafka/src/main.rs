use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::events::binlog_event::BinlogEvent;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::providers::mariadb::gtid::gtid_list::GtidList;
use mysql_cdc::providers::mysql::gtid::gtid_set::GtidSet;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use rskafka::client::error::Error;

use std::sync::Arc;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, ObjectType, TableFactor};
use chrono::{TimeZone, Utc};
use rskafka::client::partition::{OffsetAt, PartitionClient};
use rskafka::client::Client;
use rskafka::{
    client::{
        partition::{Compression, UnknownTopicHandling},
        ClientBuilder,
    },
    record::Record,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::{thread, time::Duration};

#[derive(Debug)]
struct ClientInfo {
    table_topic: String,
    partition_client: PartitionClient,
    client_offset: i64
}

impl ClientInfo {
    async fn publish_event(
        &self,
        headers: String,
        value: String,
    ) -> Result<(), Error>{
        let record = Record {
            key: None,
            value: Some(value.into_bytes()),
            headers: BTreeMap::from([("mysql_binlog_headers".to_owned(), headers.into_bytes())]),
            timestamp: Utc.timestamp_millis(42),
        };
        // println!("Published event to kafka");
        self.partition_client
            .produce(vec![record], Compression::default())
            .await?;
        Ok(())
    }
    async fn subscribe(&mut self){
        let (records, high_watermark) = self.partition_client
            .fetch_records(
                self.client_offset, // offset
                1..100_000,       // min..max bytes
                3_000,            // max wait time
            )
            .await
            .unwrap();
        self.client_offset = high_watermark;
        for record in records {
            let record_clone = record.clone();
            let timestamp = record_clone.record.timestamp;
            let value = record_clone.record.value.unwrap();
            let header = record_clone
                .record
                .headers
                .get("mysql_binlog_headers")
                .unwrap()
                .clone();

            println!("============================================== Query Event from Apache kafka for {} ====================================================",self.table_topic);
            println!();
            println!("Value: {}", String::from_utf8(value).unwrap());
            println!("Timestamp: {}", timestamp);
            println!("Headers: {}", String::from_utf8(header).unwrap());
            println!();
            println!();
        }
    }
}

struct KafkaProducer {
    client: Client,
    topic: Option<String>,
}

impl KafkaProducer {
    async fn connect(url: String) -> Self {
        KafkaProducer {
            client: ClientBuilder::new(vec![url])
                .build()
                .await
                .expect("Couldn't connect to kafka"),
            topic: None,
        }
    }

    async fn create_topic(&mut self, topic_name: &str) {
        let topics = self.client.list_topics().await.unwrap();

        for topic in topics {
            if topic.name.eq(&topic_name.to_string()) {
                self.topic = Some(topic_name.to_string());
                println!("Topic {} already exist in Kafka", topic_name.to_string());
                return;
            }
        }

        let controller_client = self
            .client
            .controller_client()
            .expect("Couldn't create controller client kafka");
        controller_client
            .create_topic(
                topic_name, 1,     // partitions
                1,     // replication factor
                5_000, // timeout (ms)
            )
            .await
            .unwrap();
        self.topic = Some(topic_name.to_string());
    }

    fn create_record(&self, headers: String, value: String) -> Record {
        Record {
            key: None,
            value: Some(value.into_bytes()),
            headers: BTreeMap::from([("mysql_binlog_headers".to_owned(), headers.into_bytes())]),
            timestamp: Utc.timestamp_millis(42),
        }
    }

    async fn get_partition_client(&self, partition: i32, topic: &str) -> Option<PartitionClient> {
        let topic = topic;
        Some(
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .expect("Couldn't fetch controller client"),
        )
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), mysql_cdc::errors::Error> {
    // Writing a constant array of strings called LOGGING_TABLES
    const LOGGING_TABLES: [&str;3] = ["users", "posts", "comments"];

    let sleep_time: u64 = std::env::var("SLEEP_TIME").unwrap().parse().unwrap();

    thread::sleep(Duration::from_millis(sleep_time));
    println!("Thread started");

    // // Start replication from MariaDB GTID
    // let _options = BinlogOptions::from_mariadb_gtid(GtidList::parse("0-1-270")?);
    //
    // // Start replication from MySQL GTID
    // let gtid_set =
    //     "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
    // let _options = BinlogOptions::from_mysql_gtid(GtidSet::parse(gtid_set)?);
    //
    // // Start replication from the position
    // let _options = BinlogOptions::from_position(String::from("mysql-bin.000008"), 195);
    //
    // Start replication from last master position.
    // Useful when you are only interested in new changes.
    let options = BinlogOptions::from_end();

    // Start replication from first event of first available master binlog.
    // Note that binlog files by default have expiration time and deleted.
    // let options = BinlogOptions::from_start();

    let username = std::env::var("SQL_USERNAME").unwrap();
    let password = std::env::var("SQL_PASSWORD").unwrap();
    let mysql_port = std::env::var("SQL_PORT").unwrap();
    let mysql_hostname = std::env::var("SQL_HOSTNAME").unwrap();
    let mysql_database = std::env::var("SQL_DATABASE").unwrap();
    

    let options = ReplicaOptions {
        username,
        password,
        port: mysql_port.parse::<u16>().unwrap(),
        hostname: mysql_hostname,
        database: Some(mysql_database.clone()),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut bin_log_client = BinlogClient::new(options);
    println!("Connected to mysql database");

    let kafka_url = std::env::var("KAFKA_URL").unwrap();
    let mut kafka_producer = KafkaProducer::connect(kafka_url).await;
    println!("Connected to kafka server");


    kafka_producer.create_topic("mysql_binlog_events").await;
    let mut clients = HashMap::new();
    for table in LOGGING_TABLES{
        let table_topic = format!("{mysql_database}_{table}");
        kafka_producer.create_topic(&table_topic).await;
        let partition_client= kafka_producer.get_partition_client(0, table_topic.as_str()).await.unwrap();
        let client_offset = partition_client.get_offset(OffsetAt::Latest).await.unwrap();
        clients.insert(table,ClientInfo{
            table_topic: table_topic,
            partition_client: partition_client,
            client_offset: client_offset
        });
    }
    
    // Standalone partition client for the mysql_binlog_events topic with its separate offset
    let partition_client_binlog = kafka_producer
        .get_partition_client(0, "mysql_binlog_events")
        .await
        .unwrap();
    let mut partition_offset_binlog = partition_client_binlog.get_offset(OffsetAt::Latest).await.unwrap();


    for result in bin_log_client.replicate()? {
        let (header, event) = result?;
        let json_event = serde_json::to_string(&event).expect("Couldn't convert sql event to json");
        let json_header = serde_json::to_string(&header).expect("Couldn't convert sql header to json");

        if let BinlogEvent::QueryEvent(value) = &event {
            // Using Generic Dialect to make the parser work with any SQL Dialect
            let dialect = GenericDialect{};
            // To uniquely store tables we use HashSet
            let mut query_tables = HashSet::new();
            let ast = Parser::parse_sql(&dialect,&value.sql_statement).expect("Could not parse SQL Statement");

            for statement in ast{
                match statement {
                    Statement::Insert{table_name, ..} => {
                        for table in table_name.0{
                            query_tables.insert(table.value);
                        }
                    }
                    Statement::CreateTable{name,..} => {
                        for table in name.0{
                            query_tables.insert(table.value);
                        }
                    }
                    Statement::Update{table,..} => {
                        match table.relation {
                            TableFactor::Table { name,..} => {
                                for t in name.0{
                                    query_tables.insert(t.value);
                                }
                            }
                            _ => {}
                        }
                    }
                    Statement::AlterTable{name,..} => {
                        for table in name.0{
                            query_tables.insert(table.value);
                        }
                    }
                    Statement::Delete{tables,..} =>{
                        for t in tables{
                            for table in t.0{
                                query_tables.insert(table.value);
                            }
                        }
                    }
                    /*  Some internal parsing error because of sqlparser
                    Statement::Drop{object_type,names,..} => {
                        match object_type {
                            ObjectType::Table =>{
                                for tables in names{
                                    for table in tables.0{
                                        query_tables.insert(table.value);
                                    }
                                }
                            }
                            _ => {}
                        }
                    
                    }
                    */
                    _ => {}
                }
            }
            for table in LOGGING_TABLES{
                if query_tables.contains(table){
                    
                    // Produce
                    clients.get(&table).unwrap().publish_event(json_header.clone(), json_event.clone()).await;
                    
                    // Consume
                    clients.get_mut(&table).unwrap().subscribe().await;
                }
            }      
        } 
        else {
            // Produce
            let kafka_record = kafka_producer.create_record(json_header.clone(), json_event.clone());
            partition_client_binlog.produce(vec![kafka_record], Compression::default()).await.unwrap();  
            
            // Consume
            let (records, high_watermark) = partition_client_binlog
            .fetch_records(
                partition_offset_binlog, // offset
                1..100_000,       // min..max bytes
                1_000,            // max wait time
            )
            .await
            .unwrap();

            partition_offset_binlog = high_watermark;

            for record in records {
                let record_clone = record.clone();
                let timestamp = record_clone.record.timestamp;
                let value = record_clone.record.value.unwrap();
                let header = record_clone
                    .record
                    .headers
                    .get("mysql_binlog_headers")
                    .unwrap()
                    .clone();

                println!("============================================== Event from Apache kafka ==========================================================================");
                println!();
                println!("Value: {}", String::from_utf8(value).unwrap());
                println!("Timestamp: {}", timestamp);
                println!("Headers: {}", String::from_utf8(header).unwrap());
                println!();
                println!();
            }          
        }
        
        // After you processed the event, you need to update replication position this was same in cdc example
        bin_log_client.commit(&header, &event);
    }
    Ok(())
}
