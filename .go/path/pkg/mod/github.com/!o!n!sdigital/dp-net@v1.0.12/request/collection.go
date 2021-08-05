package request

import (
	"github.com/ONSdigital/dp-api-clients-go/headers"
	"net/http"
)

// CollectionID header and cookie keys
const (
	CollectionIDHeaderKey = "Collection-Id"
	CollectionIDCookieKey = "collection"
)

func GetCollectionID(req *http.Request) (string, error) {
	var collectionID string

	id, err := headers.GetCollectionID(req)
	if err == nil {
		collectionID = id
	} else if headers.IsErrNotFound(err) {
		collectionID, err = getCollectionIDFromCookie(req)
	}

	return collectionID, err
}

func getCollectionIDFromCookie(req *http.Request) (string, error) {
	var collectionID string
	var err error

	c, err := req.Cookie(CollectionIDCookieKey)
	if err == nil {
		collectionID = c.Value
	} else if err == http.ErrNoCookie {
		err = nil // we don't consider this scenario an error so we set err to nil and return an empty string
	}

	return collectionID, err
}
