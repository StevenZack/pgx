package pgx

import "github.com/iancoleman/strcase"

func ToTableName(s string) string {
	s = strcase.ToSnake(s)
	if s == "user" {
		return "users"
	}
	return s
}
