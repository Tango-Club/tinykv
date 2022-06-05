package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

var (
	NotFoundResponese = &kvrpcpb.RawGetResponse{
		NotFound: true,
	}
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return NotFoundResponese, nil
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil || val == nil {
		return NotFoundResponese, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: false,
	}, nil

}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {

	limit := req.Limit
	var kvs []*kvrpcpb.KvPair

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())

	for limit > 0 && iter.Valid() {
		val, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: val,
		})
		iter.Next()
		limit--
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
