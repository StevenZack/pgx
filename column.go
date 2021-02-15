package pgx

import (
	"database/sql"
	"fmt"
)

type Column struct {
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
}

func DescTable(pool *sql.DB, database, schema, tableName string) ([]Column, error) {
	rows, e := pool.Query(`select column_name,data_type from information_schema.columns where table_catalog=$1 and table_schema=$2 and table_name=$3`, database, schema, tableName)
	if e != nil {
		return nil, e
	}

	out := []Column{}
	for rows.Next() {
		v := Column{}
		e = rows.Scan(&v.ColumnName, &v.DataType)
		if e != nil {
			break
		}
		out = append(out, v)
	}

	//check err
	if closeErr := rows.Close(); closeErr != nil {
		return nil, fmt.Errorf("rows.Close() err:%w", closeErr)
	}
	if e != nil {
		return nil, e
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return out, nil
}
