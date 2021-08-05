// This file contains a Params struct, which is used by go-ns/audit
// as part of the initial code migration, it was copied across from go-ns/common
// but when go-ns/audit is migrated to its own repository, we should also
// move this file (and its test).

package request

// Params represents a generic map of key value pairs, expected by go-ns/audit Auditor.Record()
type Params map[string]string

// Copy preserves the original params value (key value pair)
// but stores the data in a different reference address
func (originalParams Params) Copy() Params {
	if originalParams == nil {
		return nil
	}

	params := Params{}
	for key, value := range originalParams {
		params[key] = value
	}

	return params
}
