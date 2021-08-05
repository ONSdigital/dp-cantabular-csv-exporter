package request

import (
	"net/http"
	"strings"
)

const (
	LangEN = "en"
	LangCY = "cy"

	DefaultLang = LangEN

	LocaleCookieKey = "lang"
	LocaleHeaderKey = "LocaleCode"
)

var SupportedLanguages = [2]string{LangEN, LangCY}

// SetLocaleCode will fetch the locale code and then sets it
func SetLocaleCode(req *http.Request) *http.Request {
	localeCode := GetLocaleCode(req)
	req.Header.Set(LocaleHeaderKey, localeCode)

	return req
}

// GetLocaleCode will grab the locale code from the request
func GetLocaleCode(r *http.Request) string {
	locale := GetLangFromSubDomain(r)

	// Language is overridden by cookie 'lang' here if present.
	if c, err := r.Cookie(LocaleCookieKey); err == nil && len(c.Value) > 0 {
		locale = GetLangFromCookieOrDefault(c)
	}
	return locale
}

// GetLangFromSubDomain returns a language based on subdomain
func GetLangFromSubDomain(req *http.Request) string {
	args := strings.Split(req.Host, ".")
	if len(args) == 0 {
		// Defaulting to "en" (LangEN) if no arguments
		return LangEN
	}
	if args[0] == LangCY {
		return LangCY
	}
	return LangEN
}

// GetLangFromCookieOrDefault returns a language based on the lang cookie or if not valid defaults it
func GetLangFromCookieOrDefault(c *http.Cookie) string {
	if c.Value == LangCY || c.Value == LangEN {
		return c.Value
	}
	return DefaultLang
}
