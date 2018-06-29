package backends

import (
	"database/sql"
)

type resSchema struct {
	resTable string
	cols     []string
	colTypes []*sql.ColumnType

	schema string
}

// // TestResTableSchema tests the result schema generation.
// func TestResTableSchema(t *testing.T) {
// 	tests := []resSchema{
// 		resSchema{
// 			resTable: "results",
// 			cols:     []string{"c1", "c2", "c3"},
// 			colTypes: []*sql.ColumnType{
// 				*sql.ColumnType{},
// 			},
// 		},
// 	}

// 	fmt.Println(tests)
// }
