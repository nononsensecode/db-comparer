package dbcomparer

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v2"
)

type DBComparer struct {
	poolCreator PoolCreator
	connString  string
}

type PoolCreator func(ctx context.Context, connString string) (PgxIface, error)

type TableName string
type ColumnName string

func New(poolCreator PoolCreator, connString string) *DBComparer {
	if poolCreator == nil {
		panic("pool creator cannot be nil")
	}

	if strings.TrimSpace(connString) == "" {
		panic("connection string cannot be empty")
	}

	return &DBComparer{
		poolCreator: poolCreator,
		connString:  connString,
	}
}

func (c *DBComparer) Compare(datasetFile string, orderBy map[TableName][]ColumnName, ignoreFields map[TableName][]ColumnName) (matches bool, err error) {
	pool, err := c.poolCreator(context.Background(), c.connString)
	if err != nil {
		return
	}
	defer pool.Close()

	f, err := ioutil.ReadFile(datasetFile)
	if err != nil {
		return
	}

	var fileData map[TableName]interface{}
	err = yaml.Unmarshal(f, &fileData)
	if err != nil {
		return
	}

	queries := c.createQueries(fileData, orderBy)
	tableData, err := c.getTableData(queries, pool)
	if err != nil {
		return
	}

	for tableName, ymlRowData := range fileData {
		ignore := ignoreFields[tableName]
		var ymlRows []map[ColumnName]interface{}
		ymlRows, err = c.extractYMLRows(ymlRowData)
		if err != nil {
			return
		}

		var dbRows = tableData[tableName]
		if len(dbRows) != len(ymlRows) {
			panic(fmt.Sprintf("Table %s has %d rows in db and %d rows in yml", tableName, len(dbRows), len(ymlRows)))
		}

		for index, dbRow := range dbRows {
			ymlRow := ymlRows[index]
			for column, dbValue := range dbRow {
				if len(ignore) > 0 && slices.Contains(ignore, column) {
					continue
				}
				if ymlRow[column] == nil {
					err = fmt.Errorf("Table %s has no value for column %s in row %d", tableName, column, index)
				}
				ymlValue := fmt.Sprintf("%v", ymlRow[column])
				if ymlValue != dbValue {
					err = fmt.Errorf("Table %s has value %s for column %s for row %d but expected %s", tableName, dbValue, column, index, ymlValue)
					return
				}
			}
		}
	}

	return true, nil
}

func (c *DBComparer) createQueries(fileData map[TableName]interface{}, orderBy map[TableName][]ColumnName) (queries map[TableName]string) {
	queries = make(map[TableName]string)
	for k := range fileData {
		query := fmt.Sprintf(`SELECT * FROM %s`, k)
		var order []string
		if column, ok := orderBy[TableName(k)]; ok {
			for _, v := range column {
				order = append(order, string(v))
			}
		}
		if len(order) > 0 {
			query = fmt.Sprintf(`%s ORDER BY %s`, query, strings.Join(order, ", "))
		}
		queries[TableName(k)] = query
	}
	return
}

func (c *DBComparer) extractYMLRows(ymlRowData interface{}) (ymlRows []map[ColumnName]interface{}, err error) {
	for _, ymlRow := range ymlRowData.([]interface{}) {
		var row map[ColumnName]interface{}
		err = mapstructure.Decode(ymlRow, &row)
		if err != nil {
			panic(err)
		}
		ymlRows = append(ymlRows, row)
	}
	return
}

func (c *DBComparer) getTableData(queries map[TableName]string, pool PgxIface) (data map[TableName][]map[ColumnName]string, err error) {
	var rows pgx.Rows

	data = make(map[TableName][]map[ColumnName]string)

	for tableName, query := range queries {
		rows, err = pool.Query(context.Background(), query)
		if err != nil {
			return
		}
		defer rows.Close()

		var start = true
		var fields []string
		var rowValues []map[ColumnName]string
		for rows.Next() {
			if start {
				descs := rows.FieldDescriptions()
				for _, v := range descs {
					fields = append(fields, string(v.Name))
				}
				start = false
			}

			values, err := rows.Values()
			if err != nil {
				panic(err)
			}
			var rowValue = make(map[ColumnName]string)
			for i, v := range values {
				rowValue[ColumnName(fields[i])] = fmt.Sprintf("%v", v)
				if t, ok := v.(time.Time); ok {
					rowValue[ColumnName(fields[i])] = t.Format("2006-01-02 15:04:05")
				}
			}
			rowValues = append(rowValues, rowValue)
		}
		data[tableName] = rowValues
	}
	return
}
