package pgx

import (
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	"github.com/iancoleman/strcase"
)

func ToTableName(s string) string {
	s = strcase.ToSnake(s)
	if s == "user" {
		return "users"
	}
	return s
}

func toWhere(where string) string {
	where = strToolkit.TrimStart(where, " ")
	if where != "" && !strings.HasPrefix(where, "where") {
		where = " where " + where
	}
	return where
}
