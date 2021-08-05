package handlers

import (
	request "github.com/ONSdigital/dp-net/request"
)

// Key - iota enum of possible sets of keys for middleware manipulation
type Key int

// KeyMap represents a mapping between header keys, cookie keys and context keys for an equivalent value.
type KeyMap struct {
	Header  string
	Cookie  string
	Context request.ContextKey
}

// Possible values for sets of keys
const (
	UserAccess Key = iota
	Locale
	CollectionID
)

// Header returns the header key
func (k Key) Header() string {
	return KeyMaps[k].Header
}

// Cookie returns the cookie key
func (k Key) Cookie() string {
	return KeyMaps[k].Cookie
}

// Context returns the context key
func (k Key) Context() request.ContextKey {
	return KeyMaps[k].Context
}

// KeyMaps maps the possible values of Key enumeration to their Header, Cookie and Context correspnding keys
var KeyMaps = map[Key]*KeyMap{
	UserAccess: {
		Header:  request.FlorenceHeaderKey,
		Cookie:  request.FlorenceCookieKey,
		Context: request.FlorenceIdentityKey,
	},
	Locale: {
		Header:  request.LocaleHeaderKey,
		Cookie:  request.LocaleCookieKey,
		Context: request.LocaleContextKey,
	},
	CollectionID: {
		Header:  request.CollectionIDHeaderKey,
		Cookie:  request.CollectionIDCookieKey,
		Context: request.CollectionIDContextKey,
	},
}
