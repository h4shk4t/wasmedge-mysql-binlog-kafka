# Submission for pretest #2525
This is my submission for the [pretest](https://github.com/WasmEdge/WasmEdge/discussions/2525) for [LFX Mentorship on Stream Log Processing](https://github.com/WasmEdge/WasmEdge/issues/2470) issue.

# Goals
We were given the following tasks
- Make the logger only process queries for certain tables of the MySQL database
- Each binlog of a different table goes to a different topic
- Topic Naming scheme: $databaseName_$tableName

# Implementation
## Creating new topics and Kafka partition clients
The first thing we do after connecting to the Kafka Server is create all the topics in Kafka. The following loop follows the naming convention specified in the task to create the topics. It also creates a clients hashmap to map each table to its corresponding topic, partition client and offset (Part of the struct ClientInfo).

```rust
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
```
We also create a standalone partition client for the mysql_binlog_events topic with its separate offset.
```rust
    let partition_client_binlog = kafka_producer
        .get_partition_client(0, "mysql_binlog_events")
        .await
        .unwrap();
    let mut partition_offset_binlog = partition_client_binlog.get_offset(OffsetAt::Latest).await.unwrap();
```
## Processing the Binlog events, filtering them and sending them to the corresponding topics
First the Binlog event received from the replication server is checked if it matches the BinlogEvent::QueryEvent type.
The most common types of binlog events are:
- Gtid_log_event: These events contain per-transaction fields, including the GTID and logical timestamps used by MTS
- Query events: These events contain the actual SQL statements executed on the database server.
- Table Map events: These events store information about the structure of tables that are affected by the changes.
- Write Rows events: These events represent row insertions into a table.
- Update Rows events: These events represent updates to existing rows in a table.
- Delete Rows events: These events represent deletions of rows from a table.
- Xid events: These events represent transactions and indicate the end of a series of events that make up a transaction.

As we are concerned with the actual Query events for individual tables we only check against Query Events. However we can simply add more checks for other events with the `match` statement if we want to process them differently.

We get the SQL statement from the QueryEvent and parse it using the sqlparser crate. We then check if the statement is an insert, create, update or delete statement and get the table name from the statement. We then check if the table name is in the LOGGING_TABLES array and if it is we publish the event to the corresponding topic and consume it from the same client. This is done to ensure that the events are processed in order. If the table name is not in the LOGGING_TABLES array we simply ignore the event.

We have matched to see if the statement is an insert, create, update or delete statement. We have also matched for other statements like drop, alter etc. As only changes to data or schema queries are only sent out by binlog events.
```rust
    for result in bin_log_client.replicate()? {
        let (header, event) = result?;
        let json_event = serde_json::to_string(&event).expect("Couldn't convert sql event to json");
        let json_header = serde_json::to_string(&header).expect("Couldn't convert sql header to json");

        if let BinlogEvent::QueryEvent(value) = &event {
            // // Using Generic Dialect to make the parser work with any SQL Dialect
            // let dialect = GenericDialect{};
            // To uniquely store tables we use HashSet
            let mut query_tables = HashSet::new();
            let ast = Parser::parse_sql(&MySqlDialect {},&value.sql_statement).expect("Could not parse SQL Statement");

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
```
Regular events are sent normally to the mysql_binlog_events topic and are consumed by the same client.
```rust
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
```
The replication position for the binlog client is then updated as usual
```rust
        bin_log_client.commit(&header, &event);
```

## ClientInfo
The ClientInfo struct is used to store the table name, partition client and the offset for each table. It is used to publish and consume events for each table. The publish_event function is used to publish events to the corresponding topic and the subscribe function is used to consume events from the same topic. The subscribe function is called after the publish_event function to ensure that the events are processed in order. The high watermark is the offset of the last event in the topic. The high watermark is updated after each fetch_records call.
```rust
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
```

## Running the program
After building the artifact for target wasm32-wasi, the program can be run using the following command. This will show all the Events from Kafka on your terminal in realtime.
```bash
wasmedge --env "SLEEP_TIME=2000" --env "SQL_USERNAME=root" --env "SQL_PASSWORD=password" --env "SQL_PORT=3306" --env "SQL_HOSTNAME=localhost" --env "SQL_DATABASE=mysql" --env "KAFKA_URL=localhost:9092" target/wasm32-wasi/debug/mysql-binlog-kafka.wasm
```
Make sure that your MySQL, Kafka and Zookeeper services are running. (`docker compose up`)

On running the following queries on MySQL:
```
mysql> CREATE TABLE users (user_id INT PRIMARY KEY,username VARCHAR(255),email VARCHAR(255),created_at DATETIME );
Query OK, 0 rows affected (1.12 sec)

mysql> drop table users;
Query OK, 0 rows affected (0.44 sec)
```

The following events will be shown on the terminal:
```
Thread started
Connected to mysql database
Connected to kafka server
Topic mysql_binlog_events already exist in Kafka
Topic mysql_users already exist in Kafka
Topic mysql_posts already exist in Kafka
Topic mysql_comments already exist in Kafka
============================================== Event from Apache kafka ==========================================================================

Value: {"RotateEvent":{"binlog_filename":"mysql-bin.000018","binlog_position":2760}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":0,"event_type":4,"server_id":1,"event_length":47,"next_event_position":0,"event_flags":32}


============================================== Event from Apache kafka ==========================================================================

Value: {"FormatDescriptionEvent":{"binlog_version":4,"server_version":"5.7.42-log","checksum_type":"Crc32"}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":1685033227,"event_type":15,"server_id":1,"event_length":119,"next_event_position":0,"event_flags":0}


============================================== Event from Apache kafka ==========================================================================

Value: {"HeartbeatEvent":{"binlog_filename":"mysql-bin.000018"}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":0,"event_type":27,"server_id":1,"event_length":39,"next_event_position":2760,"event_flags":0}


============================================== Event from Apache kafka ==========================================================================

Value: {"MySqlGtidEvent":{"gtid":{"source_id":{"data":[68,182,183,92,248,0,17,237,181,123,2,66,172,20,0,3],"uuid":"44b6b75c-f800-11ed-b57b-0242ac140003"},"transaction_id":147},"flags":1}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":1685033806,"event_type":33,"server_id":1,"event_length":65,"next_event_position":2825,"event_flags":0}


============================================== Query Event from Apache kafka for mysql_users ====================================================

Value: {"QueryEvent":{"thread_id":153,"duration":1,"error_code":0,"status_variables":[0,0,0,0,0,1,32,0,160,85,0,0,0,0,6,3,115,116,100,4,8,0,8,0,224,0,12,1,109,121,115,113,108,0],"database_name":"mysql","sql_statement":"CREATE TABLE users (user_id INT PRIMARY KEY,username VARCHAR(255),email VARCHAR(255),created_at DATETIME )"}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":1685033806,"event_type":2,"server_id":1,"event_length":182,"next_event_position":3007,"event_flags":0}


============================================== Event from Apache kafka ==========================================================================

Value: {"MySqlGtidEvent":{"gtid":{"source_id":{"data":[68,182,183,92,248,0,17,237,181,123,2,66,172,20,0,3],"uuid":"44b6b75c-f800-11ed-b57b-0242ac140003"},"transaction_id":148},"flags":1}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":1685033833,"event_type":33,"server_id":1,"event_length":65,"next_event_position":3072,"event_flags":0}


============================================== Query Event from Apache kafka for mysql_users ====================================================

Value: {"QueryEvent":{"thread_id":153,"duration":0,"error_code":0,"status_variables":[0,0,0,0,0,1,32,0,160,85,0,0,0,0,6,3,115,116,100,4,8,0,8,0,224,0,12,1,109,121,115,113,108,0],"database_name":"mysql","sql_statement":"DROP TABLE `users` /* generated by server */"}}
Timestamp: 1970-01-01 00:00:00.042 UTC
Headers: {"timestamp":1685033833,"event_type":2,"server_id":1,"event_length":120,"next_event_position":3192,"event_flags":4}
```

Additionally you can run the kafka-console-consumer.sh script in the kafka container and you can see that each consumer receives the events from the topic it is subscribed to.
```bash
root@5e6d48b8acaf:/opt/kafka_2.13-2.8.1/bin$ kafka-console-consumer.sh --topic mysql_users --from-beginning --bootstrap-server localhost:9092
{"QueryEvent":{"thread_id":153,"duration":1,"error_code":0,"status_variables":[0,0,0,0,0,1,32,0,160,85,0,0,0,0,6,3,115,116,100,4,8,0,8,0,224,0,12,1,109,121,115,113,108,0],"database_name":"mysql","sql_statement":"CREATE TABLE users (user_id INT PRIMARY KEY,username VARCHAR(255),email VARCHAR(255),created_at DATETIME )"}}
{"QueryEvent":{"thread_id":153,"duration":0,"error_code":0,"status_variables":[0,0,0,0,0,1,32,0,160,85,0,0,0,0,6,3,115,116,100,4,8,0,8,0,224,0,12,1,109,121,115,113,108,0],"database_name":"mysql","sql_statement":"DROP TABLE `users` /* generated by server */"}}
```