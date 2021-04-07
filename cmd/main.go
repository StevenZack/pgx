package main

import (
	"fmt"
	"log"

	"github.com/StevenZack/pgx"
)

type Student struct {
	Id        uint32 `db:"id"`
	CommentId string `db:"comment_id" index:""`
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

	vs, e := m.QueryWhere("")
	if e != nil {
		log.Println(e)
		return
	}

	fmt.Println(vs)
}
