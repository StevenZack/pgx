package pgx

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	_ "github.com/lib/pq"
)

type BaseModel struct {
	Type      reflect.Type
	Dsn       string
	Database  string
	Pool      *sql.DB
	TableName string

	dbTags  []string
	dbTypes []string
}

func NewBaseModel(dsn string, data interface{}) (*BaseModel, error) {
	model, _, e := NewBaseModelWithCreated(dsn, data)
	return model, e
}

func NewBaseModelWithCreated(dsn string, data interface{}) (*BaseModel, bool, error) {
	created := false
	t := reflect.TypeOf(data)
	dsnMap, e := ParseDsn(dsn)
	if e != nil {
		return nil, false, e
	}

	model := &BaseModel{
		Dsn:       dsn,
		Type:      t,
		Database:  dsnMap["dbname"],
		TableName: ToTableName(t.Name()),
	}

	//validate
	if model.Database == "" {
		return nil, false, errors.New("dsn: dbname is not set")
	}

	//pool
	model.Pool, e = sql.Open("postgres", dsn)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, false, errors.New("data must be struct type")
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if i == 0 {
			switch field.Type.Kind() {
			case reflect.Uint,
				reflect.Uint64,
				reflect.Uint32,
				reflect.Uint16,
				reflect.String:
			default:
				return nil, false, errors.New("The first field " + field.Name + "'s type must be one of uint,uint32,uint64,uint16,string")
			}
		}

		//dbTag
		dbTag, ok := field.Tag.Lookup("db")
		if !ok {
			return nil, false, errors.New("field " + field.Name + " has no `db` tag specified")
		}
		if i == 0 && dbTag != "id" {
			return nil, false, errors.New("The first field's `db` tag must be id")
		}

		//limit
		limit := 0
		if limitStr, ok := field.Tag.Lookup("limit"); ok {
			limit, e = strconv.Atoi(limitStr)
			if e != nil {
				return nil, false, errors.New("Invalid limit tag format:" + limitStr + " for field " + field.Name)
			}
		}

		//dbType
		dbType, e := ToPostgreType(field.Type, dbTag, limit)
		if e != nil {
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
		}

		model.dbTags = append(model.dbTags, dbTag)
		model.dbTypes = append(model.dbTypes, dbType)
	}

	//desc
	columns, e := DescTable(model.Pool, model.Database, "public", model.TableName)
	if e != nil {
		return nil, false, e
	}

	if len(columns) == 0 {
		//create table
		e = model.createTable()
		if e != nil {
			return nil, false, e
		}
		created = true
	} else {
		// remote column check
		if len(model.dbTags) != len(columns) {
			return nil, false, errors.New("Inconsistent field number with remote columns: local=" + strconv.Itoa(len(model.dbTags)) + ", remote=" + strconv.Itoa(len(columns)))
		}
		for i, db := range model.dbTags {
			column := columns[i]
			if db != column.ColumnName {
				return nil, false, errors.New("Field[" + strconv.Itoa(i) + "] " + db + " doesn't match remote column:" + column.ColumnName)
			}

			dbType := model.dbTypes[i]
			dbType = strToolkit.SubBefore(dbType, " ", dbType)
			switch dbType {
			case "serial":
				dbType = "integer"
			case "smallserial":
				dbType = "smallint"
			case "bigserial":
				dbType = "bigint"
			}

			remoteType := strToolkit.SubBefore(column.DataType, " ", column.DataType)

			if dbType != remoteType {
				return nil, false, errors.New("Field[" + strconv.Itoa(i) + "] " + db + "'s type '" + dbType + "' doesn't match remote type:" + remoteType)
			}
		}
	}

	return model, created, nil
}

func (b *BaseModel) createTable() error {
	_, e := b.Pool.Exec(b.GetCreateTableSQL())
	return e
}

func (b *BaseModel) GetCreateTableSQL() string {
	builder := new(strings.Builder)
	builder.WriteString(`create table public.` + b.TableName + ` (`)
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag + " ")
		builder.WriteString(b.dbTypes[i])
		if i == 0 {
			builder.WriteString(" primary key")
		}
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(`)`)
	return builder.String()
}

func (b *BaseModel) GetInsertSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`insert into public.` + b.TableName + ` (`)

	values := new(strings.Builder)
	values.WriteString("values (")

	argsIndex := []int{}

	for i, dbTag := range b.dbTags {
		dbType := b.dbTypes[i]
		if strings.Contains(dbType, "serial") {
			continue
		}

		argsIndex = append(argsIndex, i)

		builder.WriteString(dbTag)
		values.WriteString("$" + strconv.Itoa(len(argsIndex)))

		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
			values.WriteString(",")
		}

	}

	builder.WriteString(")")
	values.WriteString(")")

	builder.WriteString(values.String())

	//returning primary key
	builder.WriteString("returning " + b.dbTags[0])

	return argsIndex, builder.String()
}

// public
func (b *BaseModel) Insert(v interface{}) (interface{}, error) {
	//validate
	value := reflect.ValueOf(v)
	t := value.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		value = value.Elem()
	}
	if t.Name() != b.Type.Name() {
		return nil, errors.New("Wrong insert type:" + t.Name() + " for table " + b.TableName)
	}

	argsIndex, query := b.GetInsertSQL()
	args := []interface{}{}
	for _, i := range argsIndex {
		args = append(args, value.Field(i).Interface())
	}

	id := reflect.New(b.Type.Field(0).Type)
	e := b.Pool.QueryRow(query, args...).Scan(id.Interface())
	if e != nil {
		return nil, e
	}

	return id.Elem().Interface(), nil
}
