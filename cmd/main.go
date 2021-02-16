package main

import (
	"fmt"
	"log"

	"github.com/StevenZack/pgx"
)

type Student struct {
	Id        uint32 `db:"id"`
	TagName   string `db:"name" index:"single=asc,unique=true,group=unique,lower=true"`
	CommentId string `db:"comment_id" index:"group=unique"`
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
