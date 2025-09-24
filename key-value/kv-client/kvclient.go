package kv_client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"raft/key-value/api"
	"strconv"
	"sync/atomic"
	"time"
)

// TODO:
//  the AppendEntries RPC sent by leaders to followers contains a "leader ID"
//  field; so followers know who the current leader is. We already have it
//  in our Raft implementation; try to plumb this information all the way
//  through to the client. When a follower sends a "I'm not a leader" response
//  to the client, it can include the ID of the service it thinks is the current
//  leader; this can reduce the search time somewhat.

var DebugClient, _ = strconv.Atoi(os.Getenv("DebugClient"))

// TODO:
//  change the clientIDs from being integers to uuid for more robustness.

type KVClient struct {
	addrs []string

	// assumedLeader is the index (in addrs) of the service we assume is the
	// current leader. It is zero-initialized by default, without loss of
	// generality.
	assumedLeader int

	// clientID is a unique identifier for a request a specific client; it's
	// managed internally by incrementing the clientCount global.
	clientID int64

	// requestID is a unique identifier for a request a specific client makes;
	// each client manages its own requestID, and increments it monotonically
	// and atomically each time the user asks to send a new request.
	requestID atomic.Int64
}

// NewClient creates a new KVClient. serviceAddrs is the addresses (each a string
// with the format "host:port") of the services in the KVService cluster the
// client will contact.
func NewClient(serviceAddrs []string) *KVClient {
	return &KVClient{
		addrs:         serviceAddrs,
		assumedLeader: 0,
		clientID:      clientCount.Add(1),
	}
}

// clientCount is used internally for debugging
var clientCount atomic.Int64

// Put the key=value pair into the store. Returns an error, or (prevValue,
// keyFound, false), where keyFound specifies whether the key was found in
// the store prior to this command, and prevValue is its previous value if
// it was found.
func (kvc *KVClient) Put(ctx context.Context, key string, value string) (string, bool, error) {
	req := &api.PutRequest{
		Key:       key,
		Value:     value,
		ClientID:  kvc.clientID,
		RequestID: kvc.requestID.Add(1),
	}
	var res api.PutResponse
	err := kvc.send(ctx, "put", req, &res)
	return res.PrevValue, res.KeyFound, err
}

// Append the value to the key in the store. Returns an error, or (prevValue,
// keyFound, false), where keyFound specifies whether the key was found in
// the store prior to this command, and prevValue is its previous value if it
// was found.
func (kvc *KVClient) Append(ctx context.Context, key string, value string) (string, bool, error) {
	appendReq := api.AppendRequest{
		Key:       key,
		Value:     value,
		ClientID:  kvc.clientID,
		RequestID: kvc.requestID.Add(1),
	}
	var appendResp api.AppendResponse
	err := kvc.send(ctx, "append", appendReq, &appendResp)
	return appendResp.PrevValue, appendResp.KeyFound, err
}

// Get the value of key from the store. Returns an error, or
// (value, found, false), where found specifies whether the key was found in
// the store, and value is its value.
func (kvc *KVClient) Get(ctx context.Context, key string) (string, bool, error) {
	req := api.GetRequest{
		Key:       key,
		ClientID:  kvc.clientID,
		RequestID: kvc.requestID.Add(1),
	}
	var resp api.GetResponse
	err := kvc.send(ctx, "get", req, &resp)
	return resp.Value, resp.KeyFound, err
}

// CAS operation: if prev value of key == compare, assign new value. Returns an
// error, or (prevValue, keyFound, false), where keyFound specifies whether the
// key was found in the store prior to this command, and prevValue is its
// previous value if it was found.
func (kvc *KVClient) CAS(ctx context.Context, key string, compare string, value string) (string, bool, error) {
	req := api.CASRequest{
		Key:          key,
		CompareValue: compare,
		Value:        value,
		ClientID:     kvc.clientID,
		RequestID:    kvc.requestID.Add(1),
	}
	var resp api.CASResponse
	err := kvc.send(ctx, "cas", req, &resp)
	return resp.PrevValue, resp.KeyFound, err
}

func (kvc *KVClient) send(ctx context.Context, route string, req any, res api.Response) error {
	// This loop rotates through the list of service addresses until we get
	// a response that indicates we've found the leader of the cluster. It
	// starts at kvc.assumedClient
FindLeader:
	for {
		// There's a two-level context tree here: we have the user context -
		// ctx, and we create our own context to impose a timeout on each
		// request to the service. If our timeout expires, we move on to try
		// the next service. In the meantime, we have to keep an eye on
		// the user context - if that's canceled at any time (due to timeout,
		// explicit cancellation, etc.), we bail out.
		retryCtx, retryCtxCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		path := fmt.Sprintf("http://%s/%s", kvc.addrs[kvc.assumedLeader], route)

		kvc.clogf("sending %#v to %v", req, path)
		if err := sendJsonRequest(retryCtx, path, req, res); err != nil {
			// Since the contexts are nested, the order of testing here matters.
			// We have to check the parent context first - if it's done, it
			// means we have to return.
			if contextDone(ctx) {
				kvc.clogf("parent context done: bailing out")
				retryCtxCancel()
				return err
			} else if contextDeadlineExceeded(retryCtx) {
				// If the parent context is not done, but our retry context is done,
				// it's time to retry a different service.
				kvc.clogf("context timed out: will try next address")
				kvc.assumedLeader = (kvc.assumedLeader + 1) % len(kvc.addrs)
				retryCtxCancel()
				continue FindLeader
			}
			retryCtxCancel()
			return err
		}
		kvc.clogf("received response %#v", res)

		// No context/timeout on this request. We've actually received a response.
		switch res.Status() {
		case api.StatusNotLeader:
			kvc.clogf("not leader: will try next address")
			kvc.assumedLeader = (kvc.assumedLeader + 1) % len(kvc.addrs)
			retryCtxCancel()
			continue FindLeader
		case api.StatusOK:
			retryCtxCancel()
			return nil
		case api.StatusFailedCommit:
			retryCtxCancel()
			return fmt.Errorf("commit failed: please retry")
		case api.StatusDuplicateRequest:
			retryCtxCancel()
			return fmt.Errorf("this request was already completed")
		default:
			panic("apparently, I am a crappy programmer. unknown state")
		}
	}
}

// sendJsonRequest sends a POST request to path with JSON payload and decodes
// the response. Encodes reqdata as JSON, sends to path, and decodes response
// into resdata.
func sendJsonRequest(ctx context.Context, path string, reqdata any, resdata any) error {
	var (
		body = new(bytes.Buffer)
		enc  = json.NewEncoder(body)
	)
	if err := enc.Encode(reqdata); err != nil {
		return fmt.Errorf("failed encoding request data. err=%w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, body)
	if err != nil {
		return fmt.Errorf("failed to create new request. err=%w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request. err=%w", err)
	}
	dec := json.NewDecoder(res.Body)
	if err := dec.Decode(resdata); err != nil {
		return fmt.Errorf("failed decoding response data. err=%w", err)
	}
	return nil
}

func (kvc *KVClient) clogf(format string, args ...any) {
	if DebugClient > 0 {
		format = fmt.Sprintf("[client:%03d] ", kvc.clientID)
		log.Printf(format, args)
	}
}

// contextDone checks whether ctx is done for any reason. It doesn't block.
func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// contextDeadlineExceeded checks whether ctx is done because of an exceeded
// deadline. It doesn't block.
func contextDeadlineExceeded(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return true
		}
	default:
	}
	return false
}
