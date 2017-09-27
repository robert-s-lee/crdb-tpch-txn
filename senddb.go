package main

import (
    "database/sql"
    "bufio"
    "os"
    "fmt"
    "log"

    _ "github.com/lib/pq"
)

func main() {

    // Connect to the database.
    db, err := sql.Open("postgres", "postgres://127.0.0.1:26257/tpch?sslmode=disable&user=root")
    if err != nil {
        log.Fatal("error connecting to the database: ", err)
    }

    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        // Insert row(s) into the table(s).
        if _, err := db.Exec(scanner.Text()); err != nil {
            log.Fatal(err)
        }
    }
}

