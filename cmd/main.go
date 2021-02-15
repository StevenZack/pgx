package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/StevenZack/pgx"
)

type Student struct {
	Id   uint32         `db:"id"`
	Name sql.NullString `db:"name"`
}

const (
	dsn = `dbname=langenius user=asd password=123456`
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	m, e := pgx.NewBaseModel(dsn, Student{})
	if e != nil {
		log.Println(e)
		return
	}
	fmt.Println(m.TableName)
}