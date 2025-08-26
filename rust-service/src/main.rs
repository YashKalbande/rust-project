use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use tokio_postgres::{NoTls, Error};
use std::env;

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
}

#[derive(Serialize)]
struct QueryResult {
    rows: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn handle_query(req: web::Json<QueryRequest>) -> impl Responder {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, connection) = match tokio_postgres::connect(&database_url, NoTls).await {
        Ok(res) => res,
        Err(e) => {
            eprintln!("Failed to connect to database: {}", e);
            return HttpResponse::InternalServerError().json(ErrorResponse {
                error: "Failed to connect to the database.".to_string(),
            });
        }
    };

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("database connection error: {}", e);
        }
    });

    // Simple check to prevent some malicious queries
    if !req.query.trim().to_lowercase().starts_with("select") {
        return HttpResponse::BadRequest().json(ErrorResponse {
            error: "Only SELECT queries are allowed.".to_string(),
        });
    }

    match client.query(&req.query, &[]).await {
        Ok(rows) => {
            let mut result_rows = Vec::new();
            for row in rows {
                let mut row_map = serde_json::Map::new();
                for (i, col) in row.columns().iter().enumerate() {
                    let value = match row.try_get::<usize, Option<String>>(i) {
                        Ok(Some(s)) => serde_json::Value::String(s),
                        Ok(None) => serde_json::Value::Null,
                        Err(_) => serde_json::Value::Null, // Handle other types gracefully
                    };
                    row_map.insert(col.name().to_string(), value);
                }
                result_rows.push(serde_json::Value::Object(row_map));
            }
            HttpResponse::Ok().json(QueryResult { rows: result_rows })
        }
        Err(e) => {
            eprintln!("Query execution error: {}", e);
            HttpResponse::BadRequest().json(ErrorResponse {
                error: e.to_string(),
            })
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(
                web::resource("/api/query") // <-- CHANGED THIS LINE
                    .route(web::post().to(handle_query))
            )
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}

// docker-compose exec postgres_db psql -U user -d dbname
// curl -X POST http://localhost:8080/api/query -H "Content-Type: application/json" -d "{\"query\":\"SELECT * FROM users;\"}"