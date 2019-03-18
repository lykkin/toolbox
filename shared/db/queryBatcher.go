package shared

import (
	"strings"

	"github.com/gocql/gocql"
)

type queryBatcher struct {
	queries       []string
	values        []interface{}
	session       *gocql.Session
	maxBatchSize  int
	ActiveQueries int
	errChan       chan error
}

func (qb *queryBatcher) CanFit(numQueries int) bool {
	return len(qb.queries)+numQueries <= qb.maxBatchSize
}

func (qb *queryBatcher) AddQuery(query string, values *[]interface{}) {
	qb.queries = append(qb.queries, query)
	qb.values = append(qb.values, *values...)
}

func (qb *queryBatcher) Reset() {
	qb.queries = make([]string, 0)
	qb.values = make([]interface{}, 0)
}

func doQuery(query string, values []interface{}, session *gocql.Session, errChan chan error) {
	errChan <- session.Query(query, values...).Exec()
}

func (qb *queryBatcher) Execute() {
	batchQuery := "BEGIN BATCH " + strings.Join(qb.queries, "") + " APPLY BATCH;"
	go doQuery(batchQuery, qb.values, qb.session, qb.errChan)
	qb.ActiveQueries++
}

func NewQueryBatcher(session *gocql.Session, outChan chan error) *queryBatcher {
	return &queryBatcher{
		queries:       make([]string, 0),
		values:        make([]interface{}, 0),
		maxBatchSize:  10, // TODO: make configurable, or get rid of this all together
		session:       session,
		ActiveQueries: 0,
		errChan:       outChan,
	}
}
