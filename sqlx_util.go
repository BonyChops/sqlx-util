package sqlxUtil

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Sqlx interface {
	Select(dest interface{}, query string, args ...interface{}) error
	Rebind(query string) string
	Exec(query string, args ...any) (sql.Result, error)
}

func SelectIn[T any](tx Sqlx, query string, args ...any) ([]T, error) {
	query, args, err := sqlx.In(
		query,
		args...,
	)
	if err != nil {
		var t []T
		return t, err
	}

	query = tx.Rebind(query)

	var results []T
	if err := tx.Select(&results, query, args...); err != nil {
		var t []T
		return t, err
	}

	return results, nil
}

func SelectInPaired[T any](tx Sqlx, baseQuery string, pairQuery string, pairedArgs [][]any) ([]T, error) {
	return SelectInPairedWithCustomArgs[T](tx, baseQuery, pairQuery, []any{}, pairedArgs)
}

func SelectInPairedWithCustomArgs[T any](tx Sqlx, baseQuery string, pairQuery string, customArgs []any, pairedArgs [][]any) ([]T, error) {
	if !strings.Contains(baseQuery, "WHERE") {
		baseQuery += " WHERE "
	}
	if len(pairedArgs) == 0 {
		var results []T
		return results, nil
	}

	conditions := make([]string, 0, len(pairedArgs))
	args := make([]any, 0, len(customArgs)+len(pairedArgs)*len(pairedArgs[0]))

	for _, arg := range customArgs {
		args = append(args, arg)
	}

	for _, arg := range pairedArgs {
		conditions = append(conditions, pairQuery)
		if len(arg) != len(pairedArgs[0]) {
			return nil, errors.New("invalid value")
		}
		for _, v := range arg {
			args = append(args, v)
		}
	}

	baseQuery += strings.Join(conditions, " OR ")

	query := tx.Rebind(baseQuery)

	var results []T
	if err := tx.Select(&results, query, args...); err != nil {
		return nil, err
	}

	return results, nil
}

var (
	bindParameterPatterns = [...]string{
		"(?)",
		"(?, ?)",
		"(?, ?, ?)",
		"(?, ?, ?, ?)",
		"(?, ?, ?, ?, ?)",
		"(?, ?, ?, ?, ?, ?)",
		"(?, ?, ?, ?, ?, ?, ?)",
		"(?, ?, ?, ?, ?, ?, ?, ?)",
		"(?, ?, ?, ?, ?, ?, ?, ?, ?)",
	}
)

func BulkInsert(tx Sqlx, query string, values [][]any) error {
	if len(values) == 0 {
		return nil
	}
	if !strings.Contains(query, "VALUES") {
		query += " VALUES "
	}

	valueArgs := make([]any, 0, len(values)*len(values[0]))
	var placeHolderPattern string

	if len(values[0]) <= len(bindParameterPatterns) {
		placeHolderPattern = bindParameterPatterns[len(values[0])-1]
	} else {
		qs := make([]string, len(values[0]))
		for i := range qs {
			qs[i] = "?"
		}

		placeHolderPattern = "(" + strings.Join(qs, ", ") + ")"
	}

	var queryStringBuilder strings.Builder
	for i, value := range values {
		if len(value) != len(values[0]) {
			return errors.New("invalid value")
		}

		queryStringBuilder.WriteString(placeHolderPattern)
		if i != len(values)-1 {
			queryStringBuilder.WriteString(", ")
		}

		for _, v := range value {
			valueArgs = append(valueArgs, v)
		}
	}

	query += queryStringBuilder.String()
	query = tx.Rebind(query)

	_, err := tx.Exec(query, valueArgs...)
	return err
}
