package fetch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wallix/awless/cloud"
	"github.com/wallix/awless/cloud/match"
	"github.com/wallix/awless/fetch"
	"github.com/wallix/awless/graph"
)

func TestFetcher(t *testing.T) {
	instances := []*graph.Resource{
		graph.InitResource("instance", "inst_1"),
		graph.InitResource("instance", "inst_2"),
	}
	subnets := []*graph.Resource{
		graph.InitResource("subnet", "sub_1"),
		graph.InitResource("subnet", "sub_2"),
	}
	funcs := map[string]fetch.Func{
		"instance": func(context.Context, cloud.FetchCache) ([]*graph.Resource, interface{}, error) {
			return instances, nil, nil
		},
		"subnet": func(context.Context, cloud.FetchCache) ([]*graph.Resource, interface{}, error) {
			return subnets, nil, nil
		},
	}

	t.Run("fetch all", func(t *testing.T) {
		gph, err := fetch.NewFetcher(funcs).Fetch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if res, _ := gph.FindOne(cloud.NewQuery("instance").Match(match.ID("inst_1"))); res == nil {
			t.Fatalf("got unexpected resource: %v", res)
		}
		if res, _ := gph.FindOne(cloud.NewQuery("instance").Match(match.ID("inst_2"))); res == nil {
			t.Fatalf("got unexpected resource: %v", res)
		}
		if res, _ := gph.FindOne(cloud.NewQuery("subnet").Match(match.ID("sub_1"))); res == nil {
			t.Fatalf("got unexpected resource: %v", res)
		}
		if res, _ := gph.FindOne(cloud.NewQuery("subnet").Match(match.ID("sub_2"))); res == nil {
			t.Fatalf("got unexpected resource: %v", res)
		}
	})

	t.Run("fetch by type", func(t *testing.T) {
		gph, err := fetch.NewFetcher(funcs).FetchByType(context.Background(), "instance")
		if err != nil {
			t.Fatal(err)
		}
		if all, _ := gph.Find(cloud.NewQuery("subnet")); len(all) != 0 {
			t.Fatal("expected empty")
		}
		if all, _ := gph.Find(cloud.NewQuery("instance")); len(all) != 2 {
			t.Fatal("expected not empty")
		}
		if res, _ := gph.FindOne(cloud.NewQuery("instance").Match(match.ID("inst_1"))); res == nil {
			t.Fatalf("got unexpected resource: %v", res)
		}
		if res, _ := gph.FindOne(cloud.NewQuery("instance").Match(match.ID("inst_2"))); res == nil {
			t.Fatalf("got unexpected resource: %v", res)
		}
	})

	t.Run("fetch unexisting type", func(t *testing.T) {
		gph, err := fetch.NewFetcher(funcs).FetchByType(context.Background(), "unexisting")
		if err == nil {
			t.Fatal(err)
		}
		if gph == nil {
			t.Fatal("expected non nil empty graph")
		}
	})

	t.Run("fetch when fetchfunc returns nils", func(t *testing.T) {
		f := fetch.NewFetcher(
			fetch.Funcs{
				"nils": func(context.Context, cloud.FetchCache) ([]*graph.Resource, interface{}, error) { return nil, nil, nil },
			},
		)

		gph, err := f.FetchByType(context.Background(), "nils")
		if err != nil {
			t.Fatal(err)
		}
		if gph == nil {
			t.Fatal("expected non nil empty graph")
		}
	})

	t.Run("fetch when fetchfunc returns error", func(t *testing.T) {
		f := fetch.NewFetcher(
			fetch.Funcs{
				"errors": func(context.Context, cloud.FetchCache) ([]*graph.Resource, interface{}, error) {
					return nil, nil, errors.New("fetch func error")
				}},
		)

		gph, err := f.FetchByType(context.Background(), "errors")
		if err == nil {
			t.Fatal(err)
		}
		if gph == nil {
			t.Fatal("expected non nil empty graph")
		}
	})
}
