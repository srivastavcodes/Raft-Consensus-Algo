// Package api defines the data structures used in the REST API between kvservice and
// clients. These structs are JSON-encoded into the body of HTTP requests and responses
// passed between services and clients.
//
// Use bespoke ResponseStatus per response instead of HTTP status codes because
// some statuses like "not leader" or "failed commit" don't have a good match in
// standard HTTP status codes.
package api

type Response interface {
	Status() ResponseStatus
}

type PutRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
}

type PutResponse struct {
	ResponseStatus ResponseStatus
	KeyFound       bool
	PrevValue      string
}

func (pr *PutResponse) Status() ResponseStatus {
	return pr.ResponseStatus
}

type AppendRequest struct {
	Key   string
	Value string

	ClientID  int64
	RequestID int64
}

type AppendResponse struct {
	ResponseStatus ResponseStatus
	KeyFound       bool
	PrevValue      string
}

func (ar *AppendResponse) Status() ResponseStatus {
	return ar.ResponseStatus
}

type GetRequest struct {
	Key string

	ClientID  int64
	RequestID int64
}

type GetResponse struct {
	ResponseStatus ResponseStatus
	KeyFound       bool
	Value          string
}

func (gr *GetResponse) Status() ResponseStatus {
	return gr.ResponseStatus
}

type CASRequest struct {
	Key          string
	CompareValue string
	Value        string

	ClientID  int64
	RequestID int64
}

type CASResponse struct {
	ResponseStatus ResponseStatus
	KeyFound       bool
	PrevValue      string
}

func (cr *CASResponse) Status() ResponseStatus {
	return cr.ResponseStatus
}

type ResponseStatus int

const (
	StatusInvalid ResponseStatus = iota
	StatusOK
	StatusNotLeader
	StatusFailedCommit
	StatusDuplicateRequest
)

var responseName = map[ResponseStatus]string{
	StatusInvalid:          "invalid",
	StatusOK:               "OK",
	StatusNotLeader:        "NotLeader",
	StatusFailedCommit:     "FailedCommit",
	StatusDuplicateRequest: "DuplicateRequest",
}

func (rs ResponseStatus) String() string {
	return responseName[rs]
}
