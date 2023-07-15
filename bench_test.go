package scan_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	scan "github.com/qsliu2017/sql-rows-scan"
	"github.com/stretchr/testify/assert"
)

// Using

/*

CREATE TABLE employee (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
);

*/

type employee struct {
	empNo     int
	birthDate time.Time
	firstName string
	lastName  string
	gender    string
	hireDate  time.Time
}

func normal_rows(rows *sql.Rows) ([]employee, error) {
	tuples := make([]employee, 0)
	for rows.Next() {
		var e employee
		if err := rows.Scan(
			&e.empNo,
			&e.birthDate,
			&e.firstName,
			&e.lastName,
			&e.gender,
			&e.hireDate,
		); err != nil {
			return nil, err
		}
		tuples = append(tuples, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tuples, nil
}

func rows1(rows *sql.Rows) ([][]any, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dest := make([]any, 0, len(types))
	for _, t := range types {
		v := reflect.New(t.ScanType())
		dest = append(dest, v.Interface())
	}

	tuples := make([][]any, 0)
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		tuple := make([]any, 0, len(dest))
		tuples = append(tuples, tuple)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tuples, nil
}

func rows2(rows *sql.Rows) ([][]any, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	tuples := make([][]any, 0)
	for rows.Next() {
		values := make([]reflect.Value, 0, len(types))
		dest := make([]any, 0, len(types))
		for _, t := range types {
			v := reflect.New(t.ScanType())
			values = append(values, v)
			dest = append(dest, v.Interface())
		}

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

func BenchmarkBaselines(b *testing.B) {
	db, err := sql.Open("mysql", "root:passwd@/employee?parseTime=true")
	assert.NoError(b, err)
	defer db.Close()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT
	emp_no,
	birth_date,
	first_name,
	last_name,
	gender,
	hire_date
	FROM employee`)
		assert.NoError(b, err)
		defer rows.Close()
		b.StartTimer()
		_, err = normal_rows(rows)
		b.StopTimer()
		assert.NoError(b, err)
	}
}

func BenchmarkScan(b *testing.B) {
	db, err := sql.Open("mysql", "root:passwd@/employee?parseTime=true")
	assert.NoError(b, err)
	defer db.Close()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT
	emp_no,
	birth_date,
	first_name,
	last_name,
	gender,
	hire_date
	FROM employee`)
		assert.NoError(b, err)
		defer rows.Close()
		b.StartTimer()
		_, err = scan.Rows(rows)
		b.StopTimer()
		assert.NoError(b, err)
	}
}

func BenchmarkScanNoConvert(b *testing.B) {
	db, err := sql.Open("mysql", "root:passwd@/employee?parseTime=true")
	assert.NoError(b, err)
	defer db.Close()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT
	emp_no,
	birth_date,
	first_name,
	last_name,
	gender,
	hire_date
	FROM employee`)
		assert.NoError(b, err)
		defer rows.Close()
		b.StartTimer()
		_, err = rows1(rows)
		b.StopTimer()
		assert.NoError(b, err)
	}
}

func BenchmarkScanConvertEach(b *testing.B) {
	db, err := sql.Open("mysql", "root:passwd@/employee?parseTime=true")
	assert.NoError(b, err)
	defer db.Close()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT
	emp_no,
	birth_date,
	first_name,
	last_name,
	gender,
	hire_date
	FROM employee`)
		assert.NoError(b, err)
		defer rows.Close()
		b.StartTimer()
		_, err = rows2(rows)
		b.StopTimer()
		assert.NoError(b, err)
	}
}
