package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/StevenZack/pgx"
)

type Student struct {
	Id   uint32         `db:"id"`
	Name sql.NullString `db:"name" limit:"5"`
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
		log.Fatal(e)
	}

	fmt.Println(m.GetCreateTableSQL())

	e = m.InsertAll([]Student{
		{
			Name: pgx.NullString("one"),
		},
		{
			Name: pgx.NullString("two12345678"),
		},
	})
	if e != nil {
		log.Println(e)
		return
	}

}
