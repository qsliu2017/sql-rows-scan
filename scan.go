package scan

import (
	"database/sql"
	"reflect"
)

func Rows(rows *sql.Rows) ([][]any, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	values := make([]reflect.Value, 0, len(types))
	dest := make([]any, 0, len(types))
	for _, t := range types {
		v := reflect.New(t.ScanType())
		values = append(values, v)
		dest = append(dest, v.Interface())
	}

	tuples := make([][]any, 0)
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		tuple := make([]any, 0, len(dest))
		for _, v := range values {
			tuple = append(tuple, v.Elem().Interface())
		}
		tuples = append(tuples, tuple)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tuples, nil
}
