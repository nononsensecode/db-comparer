package dbcomparer

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/jackc/pgconn"
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

type DBData map[Table][]map[string]interface{}

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
			return false, nil
		}

		ignoreCols := ignoreColumns[Table(tableName)]

		for i, yamlRow := range yamlRows {
			dbRow := dbRows[i]
			matched, err = c.comapreRow(yamlRow, dbRow, ignoreCols)
			if err != nil || !matched {
				return
			}
		}
	}
	return true, nil
}

func (c *DBComparer) comapreRow(yamlRow, dbRow map[string]interface{}, ignoreCols []string) (matched bool, err error) {
	for colName, yamlVal := range yamlRow {
		if slices.Contains(ignoreCols, colName) {
			continue
		}

		yamlStringVal := fmt.Sprintf("%v", yamlVal)
		var (
			dbRowVal interface{}
			ok       bool
		)
		if dbRowVal, ok = dbRow[colName]; !ok {
			err = fmt.Errorf("column %s not found in db row", colName)
			return
		}
		if t, ok := dbRowVal.(time.Time); ok {
			if !c.comapreTimeType(t, yamlStringVal) {
				return false, nil
			}
			continue
		}

		dbStringVal := fmt.Sprintf("%v", dbRowVal)
		if yamlStringVal != dbStringVal {
			fmt.Printf("%+T\n", dbRowVal)
			fmt.Printf("yaml: %s, db: %s\n", yamlStringVal, dbStringVal)
			return false, nil
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
	dbData = make(map[Table][]map[string]interface{})

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
		var fields []string
		for rows.Next() {
			if start {
				start = false
				fieldDescs := rows.FieldDescriptions()
				for _, fieldDesc := range fieldDescs {
					fields = append(fields, string(fieldDesc.Name))
				}
			}

			var values []interface{}
			values, err = rows.Values()
			if err != nil {
				return
			}

			rowData := make(map[string]interface{})
			for i, fieldName := range fields {
				rowData[fieldName] = values[i]
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
