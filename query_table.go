package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Create a table. A RethinkDB table is a collection of JSON documents.
//
// If successful, the operation returns an object: {created: 1}. If a table with
// the same name already exists, the operation throws RqlRuntimeError.
//
// Note: that you can only use alphanumeric characters and underscores for the
// table name.
//
// Optional arguments (see http://www.rethinkdb.com/api/#js:manipulating_tables-table_create for more details):
// "primary_key" (string, defaults to "id"),
// "durability" ("soft" or "hard"),
// "cache_size" and "datacenter"
//
// r.Db("database").TableCreate("table", "durability", "soft").Run(sess)
func (t RqlTerm) TableCreate(name interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"primary_key", "durability", "cache_size", "datacenter"}, optArgs)
	return newRqlTermFromPrevVal(t, "TableCreate", p.Term_TABLE_CREATE, []interface{}{name}, optArgM)
}

// Drop a table. The table and all its data will be deleted.
//
// If successful, the operation returns an object: {dropped: 1}. If the specified
// table doesn't exist a RqlRuntimeError is thrown.
func (t RqlTerm) TableDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "TableDrop", p.Term_TABLE_DROP, []interface{}{name}, map[string]interface{}{})
}

// List all table names in a database.
func (t RqlTerm) TableList() RqlTerm {
	return newRqlTermFromPrevVal(t, "TableList", p.Term_TABLE_LIST, []interface{}{}, map[string]interface{}{})
}

// Create a new secondary index on this table.
func (t RqlTerm) IndexCreate(name interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"multi"}, optArgs)
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, []interface{}{name}, optArgM)
}

// Create a new secondary index on this table based on the value of the function
// passed.
func (t RqlTerm) IndexCreateFunc(name, f interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"multi"}, optArgs)
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, []interface{}{name, funcWrap(f)}, optArgM)
}

// Delete a previously created secondary index of this table.
func (t RqlTerm) IndexDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexDrop", p.Term_INDEX_DROP, []interface{}{name}, map[string]interface{}{})
}

// List all the secondary indexes of this table.
func (t RqlTerm) IndexList() RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexList", p.Term_INDEX_LIST, []interface{}{}, map[string]interface{}{})
}
