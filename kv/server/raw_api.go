package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	reply := kvrpcpb.RawGetResponse{NotFound: true}
	if (err != nil) {
		reply.Error = err.Error()
		return &reply, err
	}

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if (err != nil) {
		reply.Error = err.Error()
		return &reply, err
	}
	reply.Value = value
	if (value == nil) {
		reply.NotFound = true
	} else {
		reply.NotFound = false
	}

	return &reply, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	m := storage.Modify{Data: storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	reply := kvrpcpb.RawPutResponse{}
	if (err != nil) {
		reply.Error = err.Error()
		return &reply, err
	}
	return &reply, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	m := storage.Modify{Data: storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	reply := kvrpcpb.RawDeleteResponse{}
	if (err != nil) {
		reply.Error = err.Error()
		return &reply, err
	}
	return &reply, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	reply := kvrpcpb.RawScanResponse{}
	if (err != nil) {
		reply.Error = err.Error()
		return &reply, err
	}
	/* open iter */
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	var start uint32 = 0
	for iter.Seek(req.GetStartKey()); iter.Valid() && start < req.GetLimit(); iter.Next() {
		value, _ := iter.Item().Value()
		reply.Kvs = append(reply.Kvs, &kvrpcpb.KvPair{Key: iter.Item().Key(), Value: value})
		start++
	}

	return &reply, nil
}
