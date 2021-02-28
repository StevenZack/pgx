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
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type BaseModel struct {
	Type      reflect.Type
	Dsn       string
	Pool      *sql.DB
	Database  string
	Schema    string
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
		log.Println(e)
		return nil, false, e
	}

	model := &BaseModel{
		Dsn:       dsn,
		Type:      t,
		Database:  dsnMap["dbname"],
		Schema:    "public",
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
		log.Println(e)
		return nil, false, e
	}

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, false, errors.New("data must be struct type")
	}

	indexes := make(map[string]string)
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

		//index
		if index, ok := field.Tag.Lookup("index"); ok {
			indexes[dbTag] = index
		}

		//limit
		limit := 0
		if limitStr, ok := field.Tag.Lookup("limit"); ok {
			limit, e = strconv.Atoi(limitStr)
			if e != nil {
				log.Println(e)
				return nil, false, errors.New("Invalid limit tag format:" + limitStr + " for field " + field.Name)
			}
		}

		//dbType
		dbType, e := ToPostgreType(field.Type, dbTag, limit)
		if e != nil {
			log.Println(e)
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
		}

		model.dbTags = append(model.dbTags, dbTag)
		model.dbTypes = append(model.dbTypes, dbType)
	}

	//desc
	columns, e := DescTable(model.Pool, model.Database, model.Schema, model.TableName)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	if len(columns) == 0 {
		//create table
		e = model.createTable()
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		created = true
		//create index
		e = model.createIndexFromField(indexes)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}

	} else {
		// remote column check
		if len(model.dbTags) != len(columns) {
			return nil, false, errors.New("Inconsistent field number with remote columns: local=" + strconv.Itoa(len(model.dbTags)) + ", remote=" + strconv.Itoa(len(columns)))
		}
		for i, db := range model.dbTags {
			column := columns[i]
			if db != column.ColumnName {
				return nil, false, errors.New("Field[" + strconv.Itoa(i) + "] " + db + " name doesn't match remote column:" + column.ColumnName)
			}

			dbType := toPgPrimitiveType(model.dbTypes[i])
			remoteType := strToolkit.SubBefore(column.DataType, " ", column.DataType)
			if strings.HasSuffix(dbType, "[]") {
				dbType = "ARRAY"
			}
			if dbType != remoteType {
				return nil, false, errors.New("Field[" + strconv.Itoa(i) + "] " + db + "'s type '" + dbType + "' doesn't match remote type:" + remoteType)
			}
		}
	}

	return model, created, nil
}

func (b *BaseModel) createTable() error {
	query := b.GetCreateTableSQL()
	_, e := b.Pool.Exec(query)
	if e != nil {
		return fmt.Errorf("%w: %s", e, query)
	}
	return nil
}

func (b *BaseModel) GetCreateTableSQL() string {
	builder := new(strings.Builder)
	builder.WriteString(`create table ` + b.Schema + `.` + b.TableName + ` (`)
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

// GetInsertSQL returns insert SQL without returning id
func (b *BaseModel) GetInsertSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`insert into ` + b.Schema + `.` + b.TableName + ` (`)

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

	return argsIndex, builder.String()
}

// GetInsertReturningSQL returns insert SQL with returning id
func (b *BaseModel) GetInsertReturningSQL() ([]int, string) {
	argsIndex, query := b.GetInsertSQL()
	return argsIndex, query + " returning " + b.dbTags[0]
}

// GetSelectSQL returns fieldIndexes, and select SQL
func (b *BaseModel) GetSelectSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`select `)
	fieldIndexes := []int{}
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag)
		fieldIndexes = append(fieldIndexes, i)
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(" from " + b.TableName)
	return fieldIndexes, builder.String()
}

// Insert inserts v (*struct or struct type)
func (b *BaseModel) Insert(v interface{}) (interface{}, error) {
	//validate
	value := reflect.ValueOf(v)
	t := value.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		value = value.Elem()
	}
	if t.String() != b.Type.String() {
		return nil, errors.New("Wrong insert type:" + t.String() + " for table " + b.TableName)
	}

	//args
	argsIndex, query := b.GetInsertReturningSQL()
	args := []interface{}{}
	for _, i := range argsIndex {
		field := value.Field(i)
		if field.Kind() == reflect.Slice {
			args = append(args, pq.Array(field.Interface()))
			continue
		}
		args = append(args, field.Interface())
	}

	//exec
	id := reflect.New(b.Type.Field(0).Type)
	e := b.Pool.QueryRow(query, args...).Scan(id.Interface())
	if e != nil {
		return nil, e
	}

	return id.Elem().Interface(), nil
}

// InsertAll inserts vs ([]*struct or []struct type)
func (b *BaseModel) InsertAll(vs interface{}) error {
	//validate
	sliceValue := reflect.ValueOf(vs)
	t := sliceValue.Type()
	if t.Kind() != reflect.Slice {
		return errors.New("Insert value is not an slice type:" + t.String())
	}
	t = t.Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.String() != b.Type.String() {
		return errors.New("Wrong insert type:" + t.String() + " for table " + b.TableName)
	}

	//prepare
	argsIndex, query := b.GetInsertSQL()

	stmt, e := b.Pool.Prepare(query)
	if e != nil {
		return e
	}
	defer stmt.Close()

	//exec
	for i := 0; i < sliceValue.Len(); i++ {
		value := sliceValue.Index(i)
		if value.Kind() == reflect.Ptr {
			value = value.Elem()
		}

		//args
		args := []interface{}{}
		for _, j := range argsIndex {
			args = append(args, value.Field(j).Interface())
		}

		_, e := stmt.Exec(args...)
		if e != nil {
			return fmt.Errorf("insert failed when insert %v:%w", value.Interface(), e)
		}
	}

	return nil
}

// Find finds a document (*struct type) by id
func (b *BaseModel) Find(id interface{}) (interface{}, error) {
	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	fieldArgs := []interface{}{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}

	query = query + ` where ` + b.dbTags[0] + `=$1`
	e := b.Pool.QueryRow(query, id).Scan(fieldArgs...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, e
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface(), nil
}

// FindWhere finds a document (*struct type) that matches 'where' condition
func (b *BaseModel) FindWhere(where string, args ...interface{}) (interface{}, error) {
	//where
	where = toWhere(where)

	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	query = query + where
	fieldArgs := []interface{}{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}
	e := b.Pool.QueryRow(query, args...).Scan(fieldArgs...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, e
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface(), nil
}

// QueryWhere queries documents ([]*struct type) that matches 'where' condition
func (b *BaseModel) QueryWhere(where string, args ...interface{}) (interface{}, error) {
	where = toWhere(where)

	fieldIndexes, query := b.GetSelectSQL()

	//query
	query = query + where
	rows, e := b.Pool.Query(query, args...)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}

	vs := reflect.New(reflect.SliceOf(reflect.PtrTo(b.Type)))
	for rows.Next() {
		v := reflect.New(b.Type)
		fieldArgs := []interface{}{}
		for _, i := range fieldIndexes {
			fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
		}
		e = rows.Scan(fieldArgs...)
		if e != nil {
			break
		}
		vs = reflect.Append(vs, v)
	}

	// check err
	if closeErr := rows.Close(); closeErr != nil {
		return nil, fmt.Errorf("rows.Close() err:%w", closeErr)
	}
	if e != nil {
		return nil, e
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs.Elem().Interface(), nil
}

func (b *BaseModel) Exists(id interface{}) (bool, error) {
	//scan
	num := 0
	query := `select 1 from ` + b.TableName + ` where ` + b.dbTags[0] + `=$1 limit 1`
	e := b.Pool.QueryRow(query, id).Scan(&num)
	if e != nil {
		if e == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel) ExistsWhere(where string, args ...interface{}) (bool, error) {
	//where
	where = toWhere(where)

	//scan
	num := 0
	query := `select 1 from ` + b.TableName + where + ` limit 1`
	e := b.Pool.QueryRow(query, args...).Scan(&num)
	if e != nil {
		if e == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel) CountWhere(where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	//scan
	var num int64
	query := `select count(*) as count from ` + b.TableName + where
	e := b.Pool.QueryRow(query, args...).Scan(&num)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return num, nil
}

func (b *BaseModel) UpdateSet(sets string, where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	query := `update ` + b.TableName + ` set ` + sets + where
	result, e := b.Pool.Exec(query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected()
}

func (b *BaseModel) Clear() error {
	query := `truncate table ` + b.TableName
	_, e := b.Pool.Exec(query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel) Truncate() error {
	return b.Clear()
}

func (b *BaseModel) Delete(id interface{}) (int64, error) {
	query := `delete from ` + b.TableName + ` where ` + b.dbTags[0] + `=$1`
	result, e := b.Pool.Exec(query, id)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected()
}

func (b *BaseModel) DeleteWhere(where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	query := `delete from ` + b.TableName + where
	result, e := b.Pool.Exec(query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected()
}
