package main

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"

	"google.golang.org/grpc"

	sched "github.com/pingcap-incubator/tinykv/proto/pkg/pdpb"
	kv "github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
)

type ctx struct {
	conn kv.TinyKvClient
	h    *kvrpcpb.Context
}

var pdconn *grpc.ClientConn
var tikvconn *grpc.ClientConn

func locate(key []byte) (*grpc.ClientConn, ctx) {
	if pdconn == nil {
		pd, err := grpc.Dial("127.0.0.1:32379", grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			panic(err)
		}
		pdconn = pd
	}

	client := sched.NewPDClient(pdconn)
	var clusterID uint64
	{
		resp, err := client.GetMembers(context.TODO(), &sched.GetMembersRequest{})

		if err != nil {
			panic(err)
		}

		clusterID = resp.Header.ClusterId
	}

	var region *metapb.Region
	var leader *metapb.Peer
	{
		resp, err := client.GetRegion(context.TODO(), &sched.GetRegionRequest{
			Header:    &sched.RequestHeader{ClusterId: clusterID},
			RegionKey: key,
		})

		if err != nil {
			panic(err)
		}

		region = resp.Region
		leader = resp.Leader
	}

	var store *metapb.Store
	{
		resp, err := client.GetStore(context.TODO(), &sched.GetStoreRequest{
			Header:  &sched.RequestHeader{ClusterId: clusterID},
			StoreId: leader.StoreId,
		})
		if err != nil {
			panic(err)
		}
		store = resp.Store
	}

	// XXX cache region and close connection in case leader changes
	if tikvconn == nil {
		conn, err := grpc.Dial(store.Address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			panic(err)
		}
		tikvconn = conn
	}

	return tikvconn, ctx{
		conn: kv.NewTinyKvClient(tikvconn),
		h: &kvrpcpb.Context{
			RegionId:    region.Id,
			RegionEpoch: region.RegionEpoch,
			Peer:        leader,
		},
	}
}

func Put(key, val []byte) {
	_, ctx := locate(key)
	_, err := ctx.conn.RawPut(context.TODO(), &kvrpcpb.RawPutRequest{
		Context: ctx.h,
		Key:     key,
		Value:   val,
		Cf:      "default",
	})

	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}
}

func Get(key []byte) []byte {
	_, ctx := locate(key)
	resp, err := ctx.conn.RawGet(context.TODO(), &kvrpcpb.RawGetRequest{
		Context: ctx.h,
		Key:     key,
		Cf:      "default",
	})

	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}
	return resp.Value
}

func main() {
	for i := 0; i < 128; i++ {
		key := []byte("hello " + fmt.Sprintf("%4d", i))
		Put(key, []byte("world "+fmt.Sprintf("%4d", i)))

		log.Infof("Get val %v", Get(key))
	}
}
