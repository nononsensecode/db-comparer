package dbcomparer

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v2"
)

type PgxIface interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	Ping(context.Context) error
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Close()
	Stat() *pgxpool.Stat
}

type PoolGetter func(ctx context.Context, connStr string) (PgxIface, error)

type Table string

type YAMLData map[Table]interface{}

type DBData map[Table][]map[string]FieldValue

type Field struct {
	Name string
	Type uint32
}

type FieldValue struct {
	Value interface{}
	Type  uint32
}

func (y YAMLData) GetTableNames() []Table {
	tables := make([]Table, 0, len(y))
	for k := range y {
		tables = append(tables, Table(k))
	}
	return tables
}

type DBComparer struct {
	poolGetter PoolGetter
	connString string
}

func New(poolGetter PoolGetter, connString string) *DBComparer {
	if poolGetter == nil {
		panic("pool getter cannot be nil")
	}

	if strings.TrimSpace(connString) == "" {
		panic("connection string cannot be empty")
	}

	return &DBComparer{
		poolGetter: poolGetter,
		connString: connString,
	}
}

func (c *DBComparer) Compare(datasetFile string, orderBy, ignoreColumns map[Table][]string) (matched bool, err error) {
	yamlData, err := c.getYAMLData(datasetFile)
	if err != nil {
		return
	}

	dbData, err := c.getDBData(yamlData, orderBy)
	if err != nil {
		return
	}

	return c.compare(yamlData, dbData, ignoreColumns)
}

func (c *DBComparer) getYAMLData(datasetFile string) (yamlData YAMLData, err error) {
	f, err := ioutil.ReadFile(datasetFile)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(f, &yamlData)
	return
}

func (c *DBComparer) compare(yamlData YAMLData, dbData DBData, ignoreColumns map[Table][]string) (matched bool, err error) {
	for tableName, yamlRowsData := range yamlData {
		var yamlRows []map[string]interface{}
		err = mapstructure.Decode(yamlRowsData, &yamlRows)
		if err != nil {
			return
		}

		dbRows := dbData[tableName]
		if len(yamlRows) != len(dbRows) {
			err = fmt.Errorf("expected %d rows in table %s, actual %d", len(yamlRows), tableName, len(dbRows))
			return false, err
		}

		ignoreCols := ignoreColumns[Table(tableName)]

		for i, yamlRow := range yamlRows {
			dbRow := dbRows[i]
			matched, err = c.comapreRow(yamlRow, dbRow, ignoreCols)
			if matched {
				continue
			}
			return
		}
	}
	return true, nil
}

func (c *DBComparer) comapreRow(yamlRow map[string]interface{}, dbRow map[string]FieldValue, ignoreCols []string) (matched bool, err error) {
	for colName, yamlVal := range yamlRow {
		if slices.Contains(ignoreCols, colName) {
			continue
		}

		yamlStringVal := fmt.Sprintf("%v", yamlVal)
		var (
			dbRowVal FieldValue
			ok       bool
		)
		if dbRowVal, ok = dbRow[colName]; !ok {
			err = fmt.Errorf("column %s not found in db row", colName)
			return
		}
		if t, ok := dbRowVal.Value.(time.Time); ok {
			if !c.comapreTimeType(t, yamlStringVal) {
				err = fmt.Errorf("for column %s, expected in db: %s, actual in db: %s", colName, yamlStringVal, t)
				return false, err
			}
			continue
		}

		if dbRowVal.Type == pgtype.JSONOID {
			yamlStringVal = strings.TrimLeft(yamlStringVal, "\t\r\n")
			isArray := len(yamlStringVal) > 0 && yamlStringVal[0] == '['
			if isArray {
				var yamlArray []map[string]interface{}
				err = json.Unmarshal([]byte(yamlStringVal), &yamlArray)
				if err != nil {
					return
				}
				yamlStringVal = fmt.Sprintf("%v", yamlArray)
			}
			if !isArray {
				var yamlMap map[string]interface{}
				err = json.Unmarshal([]byte(yamlStringVal), &yamlMap)
				if err != nil {
					return
				}
				yamlStringVal = fmt.Sprintf("%v", yamlMap)
			}
		}

		if dbRowVal.Type == pgtype.UUIDOID {
			pgtypeuuid := pgtype.UUID{}
			err = pgtypeuuid.Set(yamlStringVal)
			if err != nil {
				return
			}
			var stringUUID string
			err = pgtypeuuid.AssignTo(&stringUUID)
			if err != nil {
				return
			}
			dbRowVal.Value = stringUUID
		}

		dbStringVal := fmt.Sprintf("%v", dbRowVal.Value)
		if yamlStringVal != dbStringVal {
			err = fmt.Errorf("for column %s, expected in db: %s, actual in db: %s", colName, yamlStringVal, dbStringVal)
			return false, err
		}
	}
	return true, nil
}

func (c *DBComparer) comapreTimeType(t time.Time, tAsString string) bool {
	var formats = []string{
		t.Format(time.RFC3339Nano),
		t.Format(time.RFC3339),
		t.Format("2006-01-02 15:04:05"),
		t.Format("2006-01-02 15:04:05.999999999"),
		t.Format(time.UnixDate),
	}
	return slices.Contains(formats, tAsString)
}

func (c *DBComparer) getDBData(yamlData YAMLData, orderBy map[Table][]string) (dbData DBData, err error) {
	dbData = make(map[Table][]map[string]FieldValue)

	ctx := context.Background()

	pool, err := c.poolGetter(ctx, c.connString)
	if err != nil {
		return
	}
	defer pool.Close()

	queries := c.buildQueries(yamlData, orderBy)
	for tableName, query := range queries {
		var rows pgx.Rows
		rows, err = pool.Query(ctx, query)
		if err != nil {
			return
		}
		defer rows.Close()

		start := true
		var fields []Field
		for rows.Next() {
			if start {
				start = false
				fieldDescs := rows.FieldDescriptions()
				for _, fieldDesc := range fieldDescs {
					fields = append(fields, Field{string(fieldDesc.Name), fieldDesc.DataTypeOID})
				}
			}

			var values []interface{}
			values, err = rows.Values()
			if err != nil {
				return
			}

			rowData := make(map[string]FieldValue)
			for i, field := range fields {
				rowData[field.Name] = FieldValue{values[i], field.Type}
			}
			dbData[tableName] = append(dbData[tableName], rowData)
		}
	}
	return
}

func (c *DBComparer) buildQueries(yamlData YAMLData, orderBy map[Table][]string) (queries map[Table]string) {
	queries = make(map[Table]string)
	for tableName := range yamlData {
		queries[tableName] = c.buildQuery(tableName, orderBy[tableName])
	}
	return
}

func (c *DBComparer) buildQuery(tableName Table, orderBy []string) (query string) {
	query = "SELECT * FROM " + string(tableName)
	if len(orderBy) > 0 {
		query += " ORDER BY " + strings.Join(orderBy, ", ")
	}
	return
}
