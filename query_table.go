package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

type TableCreateOpts struct {
	PrimaryKey interface{} `gorethink:"primary_key,omitempty"`
	Durability interface{} `gorethink:"durability,omitempty"`
	CacheSize  interface{} `gorethink:"cache_size,omitempty"`
	DataCenter interface{} `gorethink:"datacenter,omitempty"`
}

func (o *TableCreateOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Create a table. A RethinkDB table is a collection of JSON documents.
//
// If successful, the operation returns an object: {created: 1}. If a table with
// the same name already exists, the operation throws RqlRuntimeError.
//
// Note: that you can only use alphanumeric characters and underscores for the
// table name.
//
// r.Db("database").TableCreate("table", "durability", "soft").Run(sess)
func (t RqlTerm) TableCreate(name interface{}, optArgs ...TableCreateOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "TableCreate", p.Term_TABLE_CREATE, []interface{}{name}, opts)
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

type IndexCreateOpts struct {
	Multi interface{} `gorethink:"multi,omitempty"`
}

func (o *IndexCreateOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Create a new secondary index on this table.
func (t RqlTerm) IndexCreate(name interface{}, optArgs ...IndexCreateOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, []interface{}{name}, opts)
}

// Create a new secondary index on this table based on the value of the function
// passed.
func (t RqlTerm) IndexCreateFunc(name, f interface{}, optArgs ...IndexCreateOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "IndexCreate", p.Term_INDEX_CREATE, []interface{}{name, funcWrap(f)}, opts)
}

// Delete a previously created secondary index of this table.
func (t RqlTerm) IndexDrop(name interface{}) RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexDrop", p.Term_INDEX_DROP, []interface{}{name}, map[string]interface{}{})
}

// List all the secondary indexes of this table.
func (t RqlTerm) IndexList() RqlTerm {
	return newRqlTermFromPrevVal(t, "IndexList", p.Term_INDEX_LIST, []interface{}{}, map[string]interface{}{})
}
