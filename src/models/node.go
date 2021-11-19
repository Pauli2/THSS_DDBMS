package models

import (
	"errors"
	"fmt"
)

// Node manages some tables defined in models/table.go
type Node struct {
	// the name of the Node, and it should be unique across the cluster
	Identifier string
	// tableName -> table
	TableMap map[string]*Table
	Constrain map[string][]uint8
}

// NewNode creates a new node with the given name and an empty set of tables
func NewNode(id string) *Node {
	return &Node{TableMap: make(map[string]*Table), Identifier: id}
}

// SayHello is an example about how to create a method that can be accessed by RPC (remote procedure call, methods that
// can be called through network from another node). RPC methods should have exactly two arguments, the first one is the
// actual argument (or an argument list), while the second one is a reference to the result.
func (n *Node) SayHello(args interface{}, reply *string) {
	// NOTICE: use reply (the second parameter) to pass the return value instead of "return" statements.
	*reply = fmt.Sprintf("Hello %s, I am Node %s", args, n.Identifier)
}

// CreateTable creates a Table on this node with the provided schema. It returns nil if the table is created
// successfully, or an error if another table with the same name already exists.
func (n *Node) CreateTable(schema *TableSchema) error {
	// check if the table already exists
	if _, ok := n.TableMap[schema.TableName]; ok {
		return errors.New("table already exists")
	}
	// create a table and store it in the map
	t := NewTable(
		schema,
		NewMemoryListRowStore(),
	)
	n.TableMap[schema.TableName] = t
	n.Constrain = make(map[string][]uint8)
	return nil
}

func (n *Node) CallCreateTable(schema *TableSchema, reply *string) {
	err := n.CreateTable(schema)
	if err == nil {
		*reply = "create table " + schema.TableName + " sucessfully"
	} else {
		*reply = "table " + schema.TableName + " already exists"
	}
}

func (n *Node) ReadConstrain(tableName string, ruleback *[]uint8) {
	if _, ok := n.Constrain[tableName]; ok {
		fmt.Println("read constrain of " + tableName + " successfully")
		// fmt.Printf("n.Constrain[tableName] : %v\n", string(n.Constrain[tableName]))
		// fmt.Printf("type of ruleback: %T\n", ruleback)
		// fmt.Printf("type of constrain: %T\n", n.Constrain[tableName])
		*ruleback = n.Constrain[tableName]
	} else {
		fmt.Println("table " + tableName + " does not exsit")
	}
}

func (n *Node) UpdateConstrain(tableinfo []interface{}, reply *string) {
	// fmt.Println(tableinfo)
	tableName := tableinfo[0].(string)
	tablerules := tableinfo[1].([]uint8)
	fmt.Println("tablerules = ", string(tablerules))
	if _, ok := n.TableMap[tableName]; ok {
		n.Constrain[tableName] = tablerules
		*reply = "update constrain of table " + tableName + " successfully"
		// fmt.Printf("n.Constrain[tableName] : %v\n", string(n.Constrain[tableName]))
		// fmt.Printf("tablerules : %v\n", string(tablerules))
	} else {
		*reply = "table " + tableName + " does not exsit"
	}
}

func (n *Node) CallInsert(params []interface{}, reply *string) {
	tableName := params[0].(string)
	row := params[1].(Row)

	err := n.Insert(tableName, &row)
	if err == nil {
		*reply = "Insert into table sucessfully"
	} else {
		*reply = "Error when inserting"
	}
}

// Insert inserts a row into the specified table, and returns nil if succeeds or an error if the table does not exist.
func (n *Node) Insert(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Insert(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// Remove removes a row from the specified table, and returns nil if succeeds or an error if the table does not exist.
// It does not concern whether the provided row exists in the table.
func (n *Node) Remove(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Remove(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// IterateTable returns an iterator of the table through which the caller can retrieve all rows in the table in the
// order they are inserted. It returns (iterator, nil) if the Table can be found, or (nil, err) if the Table does not
// exist.
func (n *Node) IterateTable(tableName string) (RowIterator, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.RowIterator(), nil
	} else {
		return nil, errors.New("no such table")
	}
}

// IterateTable returns the count of rows in a table. It returns (cnt, nil) if the Table can be found, or (-1, err)
// if the Table does not exist.
func (n *Node) count(tableName string) (int, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.Count(), nil
	} else {
		return -1, errors.New("no such table")
	}
}

// ScanTable returns all rows in a table by the specified name or nothing if it does not exist.
// This method is recommended only to be used for TEST PURPOSE, and try not to use this method in your implementation,
// but you can use it in your own test cases.
// The reason why we deprecate this method is that in practice, every table is so large that you cannot transfer a whole
// table through network all at once, so sending a whole table in one RPC is very impractical. One recommended way is to
// fetch a batch of Rows a time.
func (n *Node) ScanTable(tableName string, dataset *Dataset) {
	if t, ok := n.TableMap[tableName]; ok {
		resultSet := Dataset{}

		tableRows := make([]Row, t.Count())
		i := 0
		iterator := t.RowIterator()
		for iterator.HasNext() {
			tableRows[i] = *iterator.Next()
			i = i + 1
		}

		resultSet.Rows = tableRows
		resultSet.Schema = *t.schema
		*dataset = resultSet
	}
}
