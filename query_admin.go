package gorethink

import (
	p "github.com/dancannon/gorethink/ql2"
)

// Config can be used to read and/or update the configurations for individual
// tables or databases.
func (t Term) Config() Term {
	return constructMethodTerm(t, "Config", p.Term_CONFIG, []interface{}{}, map[string]interface{}{})
}

// Rebalance rebalances the shards of a table. When called on a database, all
// the tables in that database will be rebalanced.
func (t Term) Rebalance() Term {
	return constructMethodTerm(t, "Rebalance", p.Term_REBALANCE, []interface{}{}, map[string]interface{}{})
}

type ReconfigureOpts struct {
	Shards     interface{} `gorethink:"shards,omitempty"`
	Replicas   interface{} `gorethink:"replicas,omitempty"`
	PrimaryTag interface{} `gorethink:"primary_replicas_tag,omitempty"`
	DryRun     interface{} `gorethink:"dry_run,omitempty"`
}

func (o *ReconfigureOpts) toMap() map[string]interface{} {
	return optArgsToMap(o)
}

// Reconfigure a table's sharding and replication.
func (t Term) Reconfigure(opts ReconfigureOpts) Term {
	return constructMethodTerm(t, "Reconfigure", p.Term_RECONFIGURE, []interface{}{}, opts.toMap())
}

// Status return the status of a table
func (t Term) Status() Term {
	return constructMethodTerm(t, "Status", p.Term_STATUS, []interface{}{}, map[string]interface{}{})
}

// Wait for a table or all the tables in a database to be ready. A table may be
// temporarily unavailable after creation, rebalancing or reconfiguring. The
// wait command blocks until the given table (or database) is fully up to date.
func Wait() Term {
	return constructRootTerm("Wait", p.Term_WAIT, []interface{}{}, map[string]interface{}{})
}

// Wait for a table or all the tables in a database to be ready. A table may be
// temporarily unavailable after creation, rebalancing or reconfiguring. The
// wait command blocks until the given table (or database) is fully up to date.
func (t Term) Wait() Term {
	return constructMethodTerm(t, "Wait", p.Term_WAIT, []interface{}{}, map[string]interface{}{})
}
