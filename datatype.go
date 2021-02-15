package pgx

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/StevenZack/tools/strToolkit"
)

func ToPostgreType(t reflect.Type, dbTag string, limit int) (string, error) {
	isId := dbTag == "id"
	switch t.Kind() {
	case reflect.Int, reflect.Int64:
		return "bigint not null default 0", nil
	case reflect.Int32:
		return "integer not null default 0", nil
	case reflect.Int16:
		return "smallint not null default 0", nil
	case reflect.Uint, reflect.Uint64:
		if isId {
			return "bigserial", nil
		}
		return "bigint not null default 0 check ( " + dbTag + ">-1 )", nil
	case reflect.Uint32:
		if isId {
			return "serial", nil
		}
		return "integer not null default 0 check ( " + dbTag + ">-1 )", nil
	case reflect.Uint16:
		if isId {
			return "smallserial", nil
		}
		return "smallint not null default 0 check ( " + dbTag + ">-1 )", nil
	case reflect.Float64:
		return "double precision not null default 0", nil
	case reflect.String:
		if limit > 0 {
			return "varchar(" + strconv.Itoa(limit) + ") not null default ''", nil
		}
		return "text not null default ''", nil
	case reflect.Bool:
		return "bool not null default false", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytea", nil
		}
		if t.Elem().Kind() == reflect.String {
			return "text[]", nil
		}
	case reflect.Struct:
		switch t.String() {
		case "time.Time":
			return "timestamp with time zone not null default '0001-01-01 00:00:00", nil
		case "sql.NullString":
			if limit > 0 {
				return "varchar(" + strconv.Itoa(limit) + ")", nil
			}
			return "text", nil
		case "sql.NullBool":
			return "bool", nil
		case "sql.NullInt32":
			return "integer", nil
		case "sql.NullInt64":
			return "bigint", nil
		case "sql.NullFloat64":
			return "double precision", nil
		case "sql.NullTime":
			return "timestamp with time zone", nil
		}
	}
	return "", errors.New("unsupport field type:" + t.Name())
}

func toPgPrimitiveType(dbType string) string {
	dbType = strToolkit.SubBefore(dbType, " ", dbType)
	dbType = strToolkit.SubBefore(dbType, "(", dbType)
	switch dbType {
	case "serial":
		dbType = "integer"
	case "smallserial":
		dbType = "smallint"
	case "bigserial":
		dbType = "bigint"
	case "char", "varchar":
		dbType = "character"
	}

	return dbType
}

func NullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: true}
}

func NullInt32(i int32) sql.NullInt32 {
	return sql.NullInt32{Int32: i, Valid: true}
}

func NullInt64(i int64) sql.NullInt64 {
	return sql.NullInt64{Int64: i, Valid: true}
}

func NullBool(b bool) sql.NullBool {
	return sql.NullBool{Bool: b, Valid: true}
}

func NullFloat64(f float64) sql.NullFloat64 {
	return sql.NullFloat64{Float64: f, Valid: true}
}

func NullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: true}
}
