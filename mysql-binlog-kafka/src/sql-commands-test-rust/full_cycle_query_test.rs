use mysql::prelude::*;
use mysql::*;

#[derive(Debug, PartialEq, Eq)]
struct User {
    id: i32,
    username: String,
    email: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct Post {
    id: i32,
    user_id: i32,
    title: String,
    body: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct Comments {
    id: i32,
    post_id: i32,
    body: String,
}

#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

fn main() -> std::result::Result<(), Error> {
    let url = if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .get_db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:password@127.0.0.1:3307/mysql".to_string()
    };
    let pool = Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;

    // Let's create a table for payments. This will be ignored by the logger.
    conn.query_drop(
        r"CREATE TEMPORARY TABLE payment (
        customer_id int not null,
        amount int not null,
        account_name text
    )",
    )?;
    let payments = vec![
        Payment {
            customer_id: 1,
            amount: 2,
            account_name: None,
        },
        Payment {
            customer_id: 3,
            amount: 4,
            account_name: Some("foo".into()),
        },
        Payment {
            customer_id: 5,
            amount: 6,
            account_name: None,
        },
        Payment {
            customer_id: 7,
            amount: 8,
            account_name: None,
        },
        Payment {
            customer_id: 9,
            amount: 10,
            account_name: Some("bar".into()),
        },
    ];
    conn.exec_batch(
        r"INSERT INTO payment (customer_id, amount, account_name)
      VALUES (:customer_id, :amount, :account_name)",
        payments.iter().map(|p| {
            params! {
                "customer_id" => p.customer_id,
                "amount" => p.amount,
                "account_name" => &p.account_name,
            }
        }),
    )?;

    conn.query_drop(
        r"CREATE TABLE users (
            id INT PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            email VARCHAR(255)
        );",
    )?;

    let users = vec![
        User {
            id: 1,
            username: "Alice".into(),
            email: None,
        },
        User {
            id: 2,
            username: "John".into(),
            email: Some("johndoe@gmail.com".into()),
        },
        User {
            id: 3,
            username: "Alex".into(),
            email: None,
        },
    ];

    conn.exec_drop(
        r"INSERT INTO users (name, email) VALUES (:name, :email)",
        users.iter().map(|u| {
            params! {
                "name" => &u.username,
                "email" => &u.email,
            }
        }),
    )?;

    // Update data in the tables
    let updated_user = User {
        id: Some(1),
        username: "John Smith".into(),
        email: Some("john.smith@example.com".into()),
    };

    conn.exec_drop(
        "UPDATE users SET name = :name, email = :email WHERE id = :id",
        params! { "id" => updated_user.id, "name" => updated_user.name, "email" => updated_user.email },
    )?;

    conn.query_drop(
        r"CREATE TABLE posts (
            id INT PRIMARY KEY,
            user_id INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            body TEXT
        );",
    )?;

    let posts = vec![
        Post {
            id: 1,
            user_id: 1,
            title: "Hello, world!".into(),
            body: None,
        },
        Post {
            id: 2,
            user_id: 1,
            title: "My first Rust post".into(),
            body: Some("Hello world! This is my first post in Rust.".into()),
        },
        Post {
            id: 3,
            user_id: 3,
            title: "I love Rust".into(),
            body: Some("Rust is awesome. It's my favorite language.".into()),
        },
    ];

    conn.exec_drop(
        r"INSERT INTO posts (user_id, title, body) VALUES (:user_id, :title, :body)",
        posts.iter().map(|p| {
            params! {
                "user_id" => &p.user_id,
                "title" => &p.title,
                "body" => &p.body,
            }
        }),
    )?;

    conn.query_drop(
        r"CREATE TABLE comments (
            id INT PRIMARY KEY,
            post_id INT NOT NULL,
            body TEXT NOT NULL
        );",
    )?;

    let comments = vec![
        Comments {
            id: 1,
            post_id: 1,
            body: "Nice post!".into(),
        },
        Comments {
            id: 2,
            post_id: 1,
            body: "Good job".into(),
        },
        Comments {
            id: 3,
            post_id: 3,
            body: "I agree".into(),
        },
    ];

    conn.exec_drop(
        r"INSERT INTO comments (post_id, body) VALUES (:post_id, :body)",
        comments.iter().map(|c| {
            params! {
                "post_id" => &c.post_id,
                "body" => &c.body,
            }
        }),
    )?;

    conn.query_drop(
        r"ALTER TABLE comments ADD COLUMN timestamp DATETIME DEFAULT CURRENT_TIMESTAMP;",
    )?;

    conn.query_drop(
        r"DELETE FROM users WHERE id = 1",
    )?;

    conn.query_drop(
        r"DROP TABLE comments;",
    )?;

    conn.query_drop(
        r"DROP TABLE users;",
    )?;

    conn.query_drop(
        r"DROP TABLE posts;",
    )?;

    conn.query_drop(
        r"DROP TABLE payments;",
    )?;

    Ok(())
}