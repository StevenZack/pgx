package pgx

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
)

type (
	indexModel struct {
		unique bool
		keys   []indexKey
	}
	indexKey struct {
		lower    bool
		key      string
		sequence string
	}
)

// createIndexFromField create index with format like: map[column_name]"single=asc,unique=true,lower=true,group=unique"
func (b *BaseModel) createIndexFromField(indexes map[string]string) error {
	imodels := []indexModel{}
	groupMap := make(map[string]indexModel)
	for key, index := range indexes {
		vs, e := url.ParseQuery(strings.ReplaceAll(index, ",", "&"))
		if e != nil {
			return errors.New("field '" + key + "', invalid index tag format:" + index)
		}

		imodel := indexModel{}
		lower := false
		group := ""
		for k := range vs {
			v := vs.Get(k)
			switch k {
			case "single":
				if len(imodel.keys) > 0 {
					return errors.New("field '" + key + "': duplicated key 'single'")
				}
				indexKey := indexKey{
					key:      key,
					sequence: "asc",
				}
				if v == "desc" {
					indexKey.sequence = "desc"
				}
				imodel.keys = append(imodel.keys, indexKey)
			case "unique", "uniq":
				imodel.unique = vs.Get(k) == "true"
			case "group":
				group = vs.Get(k)
			case "lower":
				lower = v == "true"
			default:
				return errors.New("field '" + key + "', unsupported key:" + k)
			}
		}

		// normal index
		if group == "" {
			if len(imodel.keys) == 0 {
				imodel.keys = append(imodel.keys, indexKey{
					key:      key,
					sequence: "asc",
					lower:    lower,
				})
			}
			imodels = append(imodels, imodel)
			continue
		}

		//another single index
		if len(imodel.keys) > 0 {
			imodel.keys[0].lower = lower
			imodels = append(imodels, imodel)
		}

		//group index
		before, ok := groupMap[group]
		if !ok {
			before.keys = append(before.keys, indexKey{
				key:      strToolkit.SubBefore(key, ",", key),
				sequence: "asc",
				lower:    lower,
			})
			if strings.HasPrefix(group, "unique") {
				before.unique = true
			}
			groupMap[group] = before
			continue
		}

		//append
		before.keys = append(before.keys, indexKey{
			key:      key,
			sequence: "asc",
			lower:    lower,
		})
		fmt.Println("append:", before)
		groupMap[group] = before
	}

	//add group indexes
	for _, v := range groupMap {
		imodels = append(imodels, v)
	}

	if len(imodels) == 0 {
		return nil
	}

	for _, imodel := range imodels {
		builder := new(strings.Builder)
		builder.WriteString("create ")
		if imodel.unique {
			builder.WriteString("unique ")
		}
		builder.WriteString("index on " + b.Schema + "." + b.TableName + " (")
		for i, key := range imodel.keys {
			if key.lower {
				builder.WriteString("lower(" + key.key + ")")
			} else {
				builder.WriteString(key.key)
			}
			builder.WriteString(" asc")
			if i < len(imodel.keys)-1 {
				builder.WriteString(",")
			}
		}
		builder.WriteString(")")
		query := builder.String()
		_, e := b.Pool.Exec(query)
		if e != nil {
			return fmt.Errorf("%w:%s", e, query)
		}
	}
	return nil
}
