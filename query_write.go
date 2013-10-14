package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Insert JSON documents into a table. Accepts a single JSON document or an array
// of documents. You may also pass the optional argument durability with value
// 'hard' or 'soft', to override the table or query's default durability setting,
// or the optional argument return_vals, which will return the value of the row
// you're inserting (and the old value if you use upsert) when set to true.
//
//	table.Insert(map[string]interface{}{"name": "Joe", "email": "joe@example.com"}).RunWrite(sess)
//	table.Insert([]interface{}{map[string]interface{}{"name": "Joe"}, map[string]interface{}{"name": "Paul"}}).RunWrite(sess)
func (t RqlTerm) Insert(arg interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals", "cache_size", "upsert"}, optArgs)
	return newRqlTermFromPrevVal(t, "Insert", p.Term_INSERT, []interface{}{funcWrap(arg)}, optArgM)
}

// Update JSON documents in a table. Accepts a JSON document, a RQL expression,
// or a combination of the two. The optional argument durability with value
// 'hard' or 'soft' will override the table or query's default durability setting.
// The optional argument return_vals will return the old and new values of the
// row you're modifying when set to true (only valid for single-row updates).
// The optional argument non_atomic lets you permit non-atomic updates.
func (t RqlTerm) Update(arg interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals", "non_atomic"}, optArgs)
	return newRqlTermFromPrevVal(t, "Update", p.Term_UPDATE, []interface{}{funcWrap(arg)}, optArgM)
}

// Replace documents in a table. Accepts a JSON document or a RQL expression,
// and replaces the original document with the new one. The new document must
// have the same primary key as the original document. The optional argument
// durability with value 'hard' or 'soft' will override the table or query's
// default durability setting. The optional argument return_vals will return
// the old and new values of the row you're modifying when set to true (only
// valid for single-row replacements). The optional argument non_atomic lets
// you permit non-atomic updates.
func (t RqlTerm) Replace(arg interface{}, optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals", "non_atomic"}, optArgs)
	return newRqlTermFromPrevVal(t, "Replace", p.Term_REPLACE, []interface{}{funcWrap(arg)}, optArgM)
}

// Delete one or more documents from a table. The optional argument return_vals
// will return the old value of the row you're deleting when set to true (only
// valid for single-row deletes). The optional argument durability with value
// 'hard' or 'soft' will override the table or query's default durability setting.
func (t RqlTerm) Delete(optArgs ...interface{}) RqlTerm {
	optArgM := optArgsToMap([]string{"durability", "return_vals"}, optArgs)
	return newRqlTermFromPrevVal(t, "Delete", p.Term_DELETE, []interface{}{}, optArgM)
}
