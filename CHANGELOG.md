# Changelog

## V1.11 - 27 November 2013

 * Added noreply writes
 * Added the new terms `index_status`, `index_wait` and `sync`
 * Added the profile flag to the run functions
 * Optional arguments are now structs instead of key, pair strings. Almost all of the struct fields are of type interface{} as they can have terms inside them. For example: `r.TableCreateOpts{ PrimaryKey: r.Expr("index") }`
 * Returned arrays are now properly loaded into ResultRows. In the past when running `r.Expr([]interface{}{1,2,3})` would require you to use `RunRow` followed by `Scan`. You can now use `Run` followed by `ScanAll`
