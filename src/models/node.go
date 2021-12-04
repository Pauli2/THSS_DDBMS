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
	return &Node{TableMap: make(map[string]*Table), Identifier: id, Constrain: make(map[string][]uint8)}
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
		// check if column needs to be added
		if n.TableMap[schema.TableName].schema != schema {
			for _, column := range(schema.ColumnSchemas) {
				flag := true
				for _, columnOld := range(n.TableMap[schema.TableName].schema.ColumnSchemas) {
					if column == columnOld {
						flag = false
					}
				}
				if flag {
					n.TableMap[schema.TableName].schema.ColumnSchemas = append(n.TableMap[schema.TableName].schema.ColumnSchemas, column)
				}
			}
		}
		fmt.Println(n.TableMap[schema.TableName].schema.ColumnSchemas)
		return errors.New("table already exists")
	}
	// create a table and store it in the map
	t := NewTable(
		schema,
		NewMemoryListRowStore(),
	)
	n.TableMap[schema.TableName] = t
	n.Constrain[schema.TableName] = make([]uint8, 0)
	return nil
}

func (n *Node) CallCreateTable(schema *TableSchema, reply *string) {
	// start the CreateTable function
	err := n.CreateTable(schema)
	if err == nil {
		*reply = "create table " + schema.TableName + " sucessfully"
	} else {
		*reply = "table " + schema.TableName + " already exists"
	}
}

func (n *Node) ReadConstrain(tableName string, ruleback *[]uint8) {
	// read the rule in this node
	if _, ok := n.Constrain[tableName]; ok {
		//fmt.Println("read constrain of " + tableName + " successfully")
		*ruleback = n.Constrain[tableName]
	} else {
		fmt.Println("table " + tableName + " does not exsit")
	}
}

func (n *Node) UpdateConstrain(tableinfo []interface{}, reply *string) {
	// update the rule in this node
	tableName := tableinfo[0].(string)
	tablerules := tableinfo[1].([]uint8)
	fmt.Println("tablerules = ", string(tablerules))
	if _, ok := n.TableMap[tableName]; ok {
		n.Constrain[tableName] = tablerules
		*reply = "update constrain of table " + tableName + " successfully"
	} else {
		*reply = "table " + tableName + " does not exsit"
	}
}

func (n *Node) CallInsert(params []interface{}, reply *string) {
	tableName := params[0].(string)
	row := params[1].(Row)

	// start the Insert function
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

func (n *Node) CommonAttr (schemas []*TableSchema, results *[]ColumnSchema) {
	*results = []ColumnSchema{}
	lschema, rschema := schemas[0], schemas[1]
	for _, lattrinfo := range lschema.ColumnSchemas {
		for _, rattrinfo := range rschema.ColumnSchemas {
			if lattrinfo == rattrinfo {
				*results = append(*results, lattrinfo)
			}
		}
	}
}

func (n *Node) JoinAttr (schemas []*TableSchema, commonattr *[]ColumnSchema, results *[]ColumnSchema) []([]ColumnSchema) {
	lschema := schemas[0]
	rschema := schemas[1]

	*results = []ColumnSchema{}
	*results = append(*results, *commonattr...)

	lremain := []ColumnSchema{}
	rremain := []ColumnSchema{}

	fmt.Println("lsC = ", lschema.ColumnSchemas)
	fmt.Println("rsC = ", rschema.ColumnSchemas)
	for _, lattrinfo := range lschema.ColumnSchemas {
		flag := 0
		for _, cattrinfo := range *commonattr {
			if lattrinfo == cattrinfo {
				flag ++
			}
		}
		if flag == 0 {
			*results = append(*results, lattrinfo)
			lremain = append(lremain, lattrinfo)
		}
	}

	for _, rattrinfo := range rschema.ColumnSchemas {
		flag := 0
		for _, cattrinfo := range *commonattr {
			if rattrinfo == cattrinfo {
				flag ++
			}
		}
		if flag == 0 {
			*results = append(*results, rattrinfo)
			rremain = append(rremain, rattrinfo)
		}
	}
	fmt.Println("lremain = ", lremain)
	fmt.Println("rremain = ", rremain)
	return []([]ColumnSchema){lremain, rremain}
}

func (n *Node) InnerJoin(tables []*Dataset, reply *Dataset) {
	ltable, rtable := tables[0], tables[1]
	fmt.Println("ltable = ", *ltable)
	fmt.Println("rtable = ", *rtable)

	commonattr := []ColumnSchema{}
	n.CommonAttr([]*TableSchema{&ltable.Schema, &rtable.Schema}, &commonattr)
	fmt.Println("commonattr = ", commonattr)

	joinattr := []ColumnSchema{}
	remain := n.JoinAttr([]*TableSchema{&ltable.Schema, &rtable.Schema}, &commonattr, &joinattr)
	fmt.Println("remain = ", remain)
	fmt.Println("joinattr = ", joinattr)

	newdataset := Dataset{}
	newdataset.Schema.TableName = ""
	newdataset.Schema.ColumnSchemas = joinattr

	for _, lrow := range ltable.Rows {
		// add attributions of the content in lrow
		maplrow := make(map[string]interface{})
		for index, column := range ltable.Schema.ColumnSchemas {
			maplrow[column.Name] = lrow[index]
		}
		fmt.Println("maplrow = ", maplrow)
		for _, rrow := range rtable.Rows {
			maprrow := make(map[string]interface{})
			for index, column := range rtable.Schema.ColumnSchemas {
				// fmt.Printf("type of rrow: %T\n", rrow[index])
				maprrow[column.Name] = rrow[index]
			}
			fmt.Println("maprrow = ", maprrow)

			// row to insert
			newrow := make(Row, 0)
			// check whether there is unconsistence
			flag := 0
			for _, column := range commonattr {
				if maplrow[column.Name] != "" && maprrow[column.Name] != "" && maplrow[column.Name] != maprrow[column.Name] {
					flag = 1
					break
				}
			}
			if flag == 0 {
				for _, column := range commonattr {
					if maplrow[column.Name] != "" {
						newrow = append(newrow, maplrow[column.Name])
					} else if maprrow[column.Name] != "" {
						newrow = append(newrow, maprrow[column.Name])
					} else {
						newrow = append(newrow, "")
					}
				}
				for _, column := range remain[0] {
					newrow = append(newrow, maplrow[column.Name])
				}
				for _, column := range remain[1] {
					newrow = append(newrow, maprrow[column.Name])
				}
				newdataset.AppendNoRec(newrow)
			}
		}
	}
	*reply = newdataset
}

func (n *Node) OuterJoin(tables []*Dataset, reply *Dataset) {
	ltable, rtable := tables[0], tables[1]
	fmt.Println("ltable = ", *ltable)
	fmt.Println("rtable = ", *rtable)

	if len(ltable.Rows) == 0 {
		*reply = *rtable
	} else if len(rtable.Rows) == 0 {
		*reply = *ltable
	} else {
		commonattr := []ColumnSchema{}
		n.CommonAttr([]*TableSchema{&ltable.Schema, &rtable.Schema}, &commonattr)
		fmt.Println("commonattr = ", commonattr)
	
		joinattr := []ColumnSchema{}
		remain := n.JoinAttr([]*TableSchema{&ltable.Schema, &rtable.Schema}, &commonattr, &joinattr)
		fmt.Println("remain = ", remain)
		fmt.Println("joinattr = ", joinattr)

		newdataset := Dataset{}
		newdataset.Schema.TableName = ""
		newdataset.Schema.ColumnSchemas = joinattr

		for _, lrow := range ltable.Rows {
			// add attributions of the content in lrow
			maplrow := make(map[string]interface{})
			for index, column := range ltable.Schema.ColumnSchemas {
				maplrow[column.Name] = lrow[index]
			}
			fmt.Println("maplrow = ", maplrow)

			consistence := 0
			for _, rrow := range rtable.Rows {
				maprrow := make(map[string]interface{})
				for index, column := range rtable.Schema.ColumnSchemas {
					maprrow[column.Name] = rrow[index]
				}
				fmt.Println("maprrow = ", maprrow)

				// check whether there is unconsistence
				flag := 0
				for _, column := range commonattr {
					if maplrow[column.Name] != "" && maprrow[column.Name] != "" && maplrow[column.Name] != maprrow[column.Name] {
						flag = 1
						break
					}
				}
				if flag == 0 {
					consistence ++
					// row to insert
					newrow := make(Row, 0)
					for _, column := range commonattr {
						if maplrow[column.Name] != "" {
							newrow = append(newrow, maplrow[column.Name])
						} else if maprrow[column.Name] != "" {
							newrow = append(newrow, maprrow[column.Name])
						} else {
							newrow = append(newrow, "")
						}
					}
					for _, column := range remain[0] {
						newrow = append(newrow, maplrow[column.Name])
					}
					for _, column := range remain[1] {
						newrow = append(newrow, maprrow[column.Name])
					}
					newdataset.Rows = append(newdataset.Rows, newrow)
				}
			}
			if consistence == 0 {
				// row to insert
				lnewrow := make(Row, 0)
				for _, column := range commonattr {
					fmt.Println("ccolumn = ", column)
					lnewrow = append(lnewrow, maplrow[column.Name])
				}
				for _, column := range remain[0] {
					lnewrow = append(lnewrow, maplrow[column.Name])
				}
				for _, column := range remain[1] {
					fmt.Println("column = ", column)
					lnewrow = append(lnewrow, "")
				}
				newdataset.AppendNoRec(lnewrow)
			}
		}

		for _, rrow := range rtable.Rows {
			// add attributions of the content in lrow
			maprrow := make(map[string]interface{})
			for index, column := range rtable.Schema.ColumnSchemas {
				maprrow[column.Name] = rrow[index]
			}
			fmt.Println("maprrow = ", maprrow)

			consistence := 0
			for _, lrow := range ltable.Rows {
				maplrow := make(map[string]interface{})
				for index, column := range ltable.Schema.ColumnSchemas {
					maplrow[column.Name] = lrow[index]
				}
				fmt.Println("maplrow = ", maplrow)

				// check whether there is unconsistence
				flag := 0
				for _, column := range commonattr {
					if maprrow[column.Name] != "" && maplrow[column.Name] != "" && maprrow[column.Name] != maplrow[column.Name] {
						flag = 1
						break
					}
				}
				if flag == 0 {
					consistence ++
				}
			}
			if consistence == 0 {
				// row to insert
				rnewrow := make(Row, 0)
				for _, column := range commonattr {
					fmt.Println("ccolumn = ", column)
					rnewrow = append(rnewrow, maprrow[column.Name])
				}
				for _, column := range remain[0] {
					fmt.Println("column = ", column)
					rnewrow = append(rnewrow, "")
				}
				for _, column := range remain[1] {
					rnewrow = append(rnewrow, maprrow[column.Name])
				}
				newdataset.AppendNoRec(rnewrow)
			}
		}
		*reply = newdataset
	}
}
