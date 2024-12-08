package sqlxUtil

import (
	"log"
	"strconv"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3" // SQLiteドライバ
)

type Example struct {
	ID        int64  `db:"id"`
	Name      string `db:"name"`
	CreatedAt string `db:"created_at"`
}

var (
	db *sqlx.DB
)

func init() {
	testing.Init()
	var err error
	db, err = sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
}

// initDB はSQLiteデータベースを初期化します
func initDB(db *sqlx.DB) error {
	dropTableQuery := `DROP TABLE IF EXISTS example_table;`

	createTableQuery := `
	CREATE TABLE example_table (
		id INTEGER,
		name TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	// DROP TABLEを実行
	_, err := db.Exec(dropTableQuery)
	if err != nil {
		return err
	}

	// CREATE TABLEを実行
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return err
	}

	for i := 0; i < 100; i++ {
		_, err := db.Exec(`INSERT INTO example_table (id, name) VALUES (?, ?);`, i, "name"+strconv.Itoa(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func TestSelectIn(t *testing.T) {
	err := initDB(db)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	var result, result2, result3 Example
	err = db.Get(&result, "SELECT * FROM example_table WHERE id = ?", 2)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	err = db.Get(&result2, "SELECT * FROM example_table WHERE id = ?", 4)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	err = db.Get(&result3, "SELECT * FROM example_table WHERE id = ?", 6)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	results, err := SelectIn[*Example](db, "SELECT * FROM example_table WHERE id IN (?)", []int{2, 4, 6})
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	if result.ID != results[0].ID || result.Name != results[0].Name {
		t.Errorf("Expected %v, got %v", result, results[0])
	}

	if result2.ID != results[1].ID || result2.Name != results[1].Name {
		t.Errorf("Expected %v, got %v", result2, results[1])
	}

	if result3.ID != results[2].ID || result3.Name != results[2].Name {
		t.Errorf("Expected %v, got %v", result3, results[2])
	}
}

func TestSelectInPaired(t *testing.T) {
	err := initDB(db)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	var result, result2, result3 Example
	err = db.Get(&result, "SELECT * FROM example_table WHERE id = ? AND name = ?", 2, "name2")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	err = db.Get(&result2, "SELECT * FROM example_table WHERE id = ? AND name = ?", 4, "name4")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	err = db.Get(&result3, "SELECT * FROM example_table WHERE id = ? AND name = ?", 6, "name6")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	results, err := SelectInPaired[*Example](
		db,
		"SELECT * FROM example_table",
		"(id = ? AND name = ?)",
		[][]any{
			[]any{2, "name2"},
			[]any{4, "name4"},
			[]any{6, "name6"},
		})
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	if result.ID != results[0].ID || result.Name != results[0].Name {
		t.Errorf("Expected %v, got %v", result, results[0])
	}

	if result2.ID != results[1].ID || result2.Name != results[1].Name {
		t.Errorf("Expected %v, got %v", result2, results[1])
	}

	if result3.ID != results[2].ID || result3.Name != results[2].Name {
		t.Errorf("Expected %v, got %v", result3, results[2])
	}
}

func TestBulkInsert(t *testing.T) {
	err := initDB(db)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err := db.Exec(`INSERT INTO example_table (id, name) VALUES (?, ?);`, i+200, "name"+strconv.Itoa(i+200))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	var results []Example
	err = db.Select(&results, "SELECT * FROM example_table")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	err = initDB(db)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	values := make([][]any, 100)
	for i := range values {
		values[i] = []any{i + 200, "name" + strconv.Itoa(i+200)}
	}

	err = BulkInsert(db, "INSERT INTO example_table (id, name)", values)
	if err != nil {
		t.Fatalf("Failed to bulk insert: %v", err)
	}

	var results2 []Example
	err = db.Select(&results2, "SELECT * FROM example_table")

	if len(results) != len(results2) {
		t.Fatalf("Expected %d results, got %d", len(results), len(results2))
	}

	for i := range results {
		if results[i].ID != results2[i].ID || results[i].Name != results2[i].Name {
			t.Errorf("Expected %v, got %v", results[i], results2[i])
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	values := make([][]any, 100)
	for i := range values {
		_, err := db.Exec(`INSERT INTO example_table (id, name) VALUES (?, ?);`, i+200, "name"+strconv.Itoa(i+200))
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
}

func BenchmarkBulkInsert(b *testing.B) {
	values := make([][]any, 100)
	for i := range values {
		values[i] = []any{i + 200, "name" + strconv.Itoa(i+200)}
	}

	err := BulkInsert(db, "INSERT INTO example_table (id, name)", values)
	if err != nil {
		b.Fatalf("Failed to bulk insert: %v", err)
	}
}
