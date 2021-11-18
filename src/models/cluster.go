package models

import (
	"encoding/json"
	"fmt"
	"strconv"

	"../labgob"
	"../labrpc"
)

// Cluster consists of a group of nodes to manage distributed tables defined in models/table.go.
// The Cluster object itself can also be viewed as the only coordinator of a cluster, which means client requests
// should go through it instead of the nodes.
// Of course, it is possible to make any of the nodes a coordinator and to make the cluster decentralized. You are
// welcomed to make such changes and may earn some extra points.
type Cluster struct {
	// the identifiers of each node, we use simple numbers like "1,2,3" to register the nodes in the network
	// needless to say, each identifier should be unique
	nodeIds []string
	// the network that the cluster works on. It is not actually using the network interface, but a network simulator
	// using SEDA (google it if you have not heard about it), which allows us (and you) to inject some network failures
	// during tests. Do remember that network failures should always be concerned in a distributed environment.
	network *labrpc.Network
	// the Name of the cluster, also used as a network address of the cluster coordinator in the network above
	Name string
	TableSchemaMap map[string]TableSchema
}

// NewCluster creates a Cluster with the given number of nodes and register the nodes to the given network.
// The created cluster will be named with the given one, which will used when a client wants to connect to the cluster
// and send requests to it. WARNING: the given name should not be like "Node0", "Node1", ..., as they will conflict
// with some predefined names.
// The created nodes are identified by simple numbers starting from 0, e.g., if we have 3 nodes, the identifiers of the
// three nodes will be "Node0", "Node1", and "Node2".
// Each node is bound to a server in the network which follows the same naming rule, for the example above, the three
// nodes will be bound to  servers "Node0", "Node1", and "Node2" respectively.
// In practice, we may mix the usages of terms "Node" and "Server", both of them refer to a specific machine, while in
// the lab, a "Node" is responsible for processing distributed affairs but a "Server" simply receives messages from the
// net work.
func NewCluster(nodeNum int, network *labrpc.Network, clusterName string) *Cluster {
	labgob.Register(TableSchema{})
	labgob.Register(Row{})

	nodeIds := make([]string, nodeNum)
	nodeNamePrefix := "Node"
	for i := 0; i < nodeNum; i++ {
		// identify the nodes with "Node0", "Node1", ...
		node := NewNode(nodeNamePrefix + strconv.Itoa(i))
		nodeIds[i] = node.Identifier
		// use go reflection to extract the methods in a Node object and make them as a service.
		// a service can be viewed as a list of methods that a server provides.
		// due to the limitation of the framework, the extracted method must only have two parameters, and the first one
		// is the actual argument list, while the second one is the reference to the result.
		// NOTICE, a REFERENCE should be passed to the method instead of a value
		nodeService := labrpc.MakeService(node)
		// create a server, a server is responsible for receiving requests and dispatching them
		server := labrpc.MakeServer()
		// add the service to the server so the server can provide the services
		server.AddService(nodeService)
		// register the server to the network as "Node0", "Node1", ...
		network.AddServer(nodeIds[i], server)
	}

	// create a cluster with the nodes and the network
	c := &Cluster{nodeIds: nodeIds, network: network, Name: clusterName, TableSchemaMap: make(map[string]TableSchema)}
	// create a coordinator for the cluster to receive external requests, the steps are similar to those above.
	// notice that we use the reference of the cluster as the name of the coordinator server,
	// and the names can be more than strings.
	clusterService := labrpc.MakeService(c)
	server := labrpc.MakeServer()
	server.AddService(clusterService)
	network.AddServer(clusterName, server)
	return c
}

// SayHello is an example to show how the coordinator communicates with other nodes in the cluster.
// Any method that can be accessed by network clients should have EXACTLY TWO parameters, while the first one is the
// actual parameter desired by the method (can be a list if there are more than one desired parameters), and the second
// one is a reference to the return value. The caller must ensure that the reference is valid (not nil).
func (c *Cluster) SayHello(visitor string, reply *string) {
	endNamePrefix := "InternalClient"
	for _, nodeId := range c.nodeIds {
		// create a client (end) to each node
		// the name of the client should be unique, so we use the name of each node for it
		endName := endNamePrefix + nodeId
		end := c.network.MakeEnd(endName)
		// connect the client to the node
		c.network.Connect(endName, nodeId)
		// a client should be enabled before being used
		c.network.Enable(endName, true)
		// call method on that node
		argument := visitor
		reply := ""
		// the first parameter is the name of the method to be called, recall that we use the reference of
		// a Node object to create a service, so the first part of the parameter will be the class name "Node", and as
		// we want to call the method SayHello(), so the second part is "SayHello", and the two parts are separated by
		// a dot
		end.Call("Node.SayHello", argument, &reply)
		fmt.Println(reply)
	}
	*reply = fmt.Sprintf("Hello %s, I am the coordinator of %s", visitor, c.Name)
}

// Join all tables in the given list using NATURAL JOIN (join on the common columns), and return the joined result
// as a list of rows and set it to reply.
func (c *Cluster) Join(tableNames []string, reply *Dataset) {
	//TODO lab2
}

func (c *Cluster) BuildTable(params []interface{}, reply *string) {
	schema := params[0].(TableSchema)
	rules := params[1].([]uint8)
	// fmt.Println("Schema name: ", schema.TableName)
	// fmt.Println("Schema ColumnSchemas: ", schema.ColumnSchemas)
	// fmt.Println("rules = ", string(rules))

	c.TableSchemaMap[schema.TableName] = schema
	var jsonrules map[string](map[string]interface{})
	json.Unmarshal([]uint8(rules), &jsonrules)
	// fmt.Println("jsonrules['0']['column'] = ", jsonrules["0"]["column"])
	// fmt.Println(reflect.TypeOf(jsonrules["0"]["column"]))

	for index, rule := range jsonrules {
		//fmt.Println(index, rules)
		intinddex, _ := strconv.Atoi(index)
		fmt.Println(c.nodeIds[intinddex])

		endName := "PROJ" + c.nodeIds[intinddex]
		fmt.Println(endName)
		end := c.network.MakeEnd(endName)
		c.network.Connect(endName, c.nodeIds[intinddex])
		c.network.Enable(endName, true)

		var columns []ColumnSchema
		if rule_columns, ok := rule["column"].([]interface{}); ok {
			for _, column_name := range rule_columns {
				for _, column := range schema.ColumnSchemas {
					if column.Name == column_name {
						columns = append(columns, column)
					}
				}
			}
			projtableschema := TableSchema{
				TableName: "PROJ" + index,
				ColumnSchemas: columns,
			}
			//TODO: Create a node and a table, using table_schema
			fmt.Println("table schema: ", projtableschema)
			*reply = ""
			end.Call("Node.CallCreateTable", &projtableschema, reply)
			fmt.Println("reply = ", *reply)

			rulecoef, _ := json.Marshal(rule)
			*reply = ""
			end.Call("Node.UpdateConstrain", []interface{}{projtableschema.TableName, rulecoef}, &reply)
			fmt.Println("reply = ", *reply)
			
			//var ruleback *[]uint8
			//end.Call("Node.ReadConstrain", projtableschema.TableName, &ruleback)
			//fmt.Println("ruleback = ", string(*ruleback))
		}
	}
}

func numtrans(input interface{}) float64 {
	if result, ok := input.(int); ok {
		return float64(result)
	}
	if result, ok := input.(float32); ok {
		return float64(result)
	}
	if result, ok := input.(float64); ok {
		return result
	}
	if result, ok := input.(uint); ok {
		return float64(result)
	}
	fmt.Println("Unexpected number type!")
	return 0xFFFFFFFF
}

func numcompare(value float64, crivalue float64, op string) bool {
	switch op {
	case ">":
		return value > crivalue
	case ">=":
		return value >= crivalue
	case "<=":
		return value <= crivalue
	case "<":
		return value < crivalue
	case "!=":
		return value != crivalue
	default:
		return true
	}
}

func strcompare(value string, crivalue string, op string) bool {
	switch op {
	case "!=":
		return value != crivalue
	case "==":
		return value == crivalue
	default:
		return true
	}
}

func satisfy(mapdata map[string]interface{}, condition map[string]interface{}) bool {
	for field, expressions := range condition {
		for _, expression := range expressions.([]interface{}) {
			value := mapdata[field]
			exp := expression.(map[string]interface{})
			fmt.Println(exp)
			if expression, ok := expression.(map[string]interface{}); ok {  // bug with expression!
				crivalue := expression["val"]
				op := expression["op"].(string)
				crivalue_str, ok := crivalue.(string) 
				if ok {
					if !strcompare(value.(string), crivalue_str, op) {
						return false
					}
				} else {
					if !numcompare(numtrans(value), numtrans(crivalue), op) {
						return false
					}
				}
			}
		}
	}
	return true
}

func (c *Cluster) FragmentWrite(params []interface{}, reply *string) {
	tableName := params[0].(string)
	row := params[1].(Row)
	fmt.Println("\n FragmentWriting...")
	//fmt.Println("tableName: ", tableName)
	fmt.Println("row: ", row)

	fullSchema := c.TableSchemaMap[tableName].ColumnSchemas
	//fmt.Println("Full table schema: ", fullSchema)

	for _, nodeId := range c.nodeIds {
		endName := "FW" + nodeId
		end := c.network.MakeEnd(endName)
		c.network.Connect(endName, nodeId)
		c.network.Enable(endName, true)

		rule := []uint8{}
		end.Call("Node.ReadConstrain", tableName, &rule)
		var jsonrule map[string]interface{}
		json.Unmarshal([]uint8(rule), &jsonrule)
		fmt.Println("rule of node: ", jsonrule)

		maprow := make(map[string]interface{})
		for index, schema := range fullSchema {
			maprow[schema.Name] = row[index]
		}
		//fmt.Println("maprow: ", maprow)
		reply := ""
		
		if satisfy(maprow, jsonrule["predicate"].(map[string]interface{})) {
			//TODO: Insert into table
			var rowToInsert Row
			for _, column := range jsonrule["column"].([]interface{}) {
				rowToInsert = append(rowToInsert, maprow[column.(string)])
			}
			end.Call("Node.CallInsert", []interface{}{tableName, rowToInsert}, &reply)
		}
	}
}
