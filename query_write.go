package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

type InsertOpts struct {
	Durability interface{} `gorethink:"durability,omitempty"`
	ReturnVals interface{} `gorethink:"return_vals,omitempty"`
	CacheSize  interface{} `gorethink:"cache_size,omitempty"`
	Upsert     interface{} `gorethink:"upsert,omitempty"`
}

func (o *InsertOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Insert JSON documents into a table. Accepts a single JSON document or an array
// of documents. You may also pass the optional argument durability with value
// 'hard' or 'soft', to override the table or query's default durability setting,
// or the optional argument return_vals, which will return the value of the row
// you're inserting (and the old value if you use upsert) when set to true.
//
//	table.Insert(map[string]interface{}{"name": "Joe", "email": "joe@example.com"}).RunWrite(sess)
//	table.Insert([]interface{}{map[string]interface{}{"name": "Joe"}, map[string]interface{}{"name": "Paul"}}).RunWrite(sess)
func (t RqlTerm) Insert(arg interface{}, optArgs ...InsertOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "Insert", p.Term_INSERT, []interface{}{funcWrap(arg)}, opts)
}

type UpdateOpts struct {
	Durability interface{} `gorethink:"durability,omitempty"`
	ReturnVals interface{} `gorethink:"return_vals,omitempty"`
	NotAtomic  interface{} `gorethink:"non_atomic,omitempty"`
}

func (o *UpdateOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Update JSON documents in a table. Accepts a JSON document, a RQL expression,
// or a combination of the two. The optional argument durability with value
// 'hard' or 'soft' will override the table or query's default durability setting.
// The optional argument return_vals will return the old and new values of the
// row you're modifying when set to true (only valid for single-row updates).
// The optional argument non_atomic lets you permit non-atomic updates.
func (t RqlTerm) Update(arg interface{}, optArgs ...UpdateOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "Update", p.Term_UPDATE, []interface{}{funcWrap(arg)}, opts)
}

type ReplaceOpts struct {
	Durability interface{} `gorethink:"durability,omitempty"`
	ReturnVals interface{} `gorethink:"return_vals,omitempty"`
	NotAtomic  interface{} `gorethink:"non_atomic,omitempty"`
}

func (o *ReplaceOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Replace documents in a table. Accepts a JSON document or a RQL expression,
// and replaces the original document with the new one. The new document must
// have the same primary key as the original document. The optional argument
// durability with value 'hard' or 'soft' will override the table or query's
// default durability setting. The optional argument return_vals will return
// the old and new values of the row you're modifying when set to true (only
// valid for single-row replacements). The optional argument non_atomic lets
// you permit non-atomic updates.
func (t RqlTerm) Replace(arg interface{}, optArgs ...ReplaceOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "Replace", p.Term_REPLACE, []interface{}{funcWrap(arg)}, opts)
}

type DeleteOpts struct {
	Durability interface{} `gorethink:"durability,omitempty"`
	ReturnVals interface{} `gorethink:"return_vals,omitempty"`
}

func (o *DeleteOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Delete one or more documents from a table. The optional argument return_vals
// will return the old value of the row you're deleting when set to true (only
// valid for single-row deletes). The optional argument durability with value
// 'hard' or 'soft' will override the table or query's default durability setting.
func (t RqlTerm) Delete(optArgs ...DeleteOpts) RqlTerm {
	opts := map[string]interface{}{}
	if len(optArgs) >= 1 {
		opts = optArgs[0].toMap()
	}
	return newRqlTermFromPrevVal(t, "Delete", p.Term_DELETE, []interface{}{}, opts)
}

// Sync ensures that writes on a given table are written to permanent storage.
// Queries that specify soft durability (Durability: "soft") do not give such
// guarantees, so sync can be used to ensure the state of these queries. A call
// to sync does not return until all previous writes to the table are persisted.
func (t RqlTerm) Sync() RqlTerm {
	return newRqlTermFromPrevVal(t, "Sync", p.Term_SYNC, []interface{}{}, map[string]interface{}{})
}
